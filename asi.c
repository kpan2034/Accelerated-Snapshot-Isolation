/*
 * contrib/asi/asi.c
 *
 * Copyright (c) 2023	Keyur Panchal
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose, without fee, and without a
 * written agreement is hereby granted, provided that the above
 * copyright notice and this paragraph and the following two
 * paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT,
 * INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
 * IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE,
 * SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

#include "postgres.h"

#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/latch.h"
#include "pgstat.h"
 #include "utils/guc.h"

PG_MODULE_MAGIC;

extern HTAB *LockMethodLockHash;

PG_FUNCTION_INFO_V1(asi_try_exclusive_lock);
PG_FUNCTION_INFO_V1(asi_try_shared_lock);
PG_FUNCTION_INFO_V1(asi_try_intent_lock);
PG_FUNCTION_INFO_V1(asi_try_table_exclusive_lock);
PG_FUNCTION_INFO_V1(asi_validate_intent_lock);
PG_FUNCTION_INFO_V1(asi_release_intent_lock);
PG_FUNCTION_INFO_V1(asi_release_exclusive_lock);

// don't need these since the above functions all acquire transaction locks
// PG_FUNCTION_INFO_V1(asi_short_unlock);
// PG_FUNCTION_INFO_V1(asi_long_unlock);

void sleep(float8 secs);
static void release_intent_locks(LOCKTAG*locktag);

#define SET_LOCKTAG(tag, key1, key2) \
  SET_LOCKTAG_ADVISORY(tag, MyDatabaseId, key1, key2, 2)
#define MAX_RETRIES 3
#define TIMEOUT_MS 50
static HTAB* LOCKHTABLE;

typedef struct 
{
  // store the tag value as well
  LOCKTAG tag;
  // indicates number of AccessShareLocks on this table entry
  int nlocks;
} LockTableEntry;

void
_PG_init(void)
{
  // You probably need to figure out how to put aside some shared memory for your hash table here

  /* Set up hash table here */

  /* Can use GUC here to setup timeouts, etc */
  // DefineCustomIntVariable();

  

  // Unlike the LockMethodLockHash used in lock.c
  // here we expect that each LOCKTAG has only a single entry
  // in LOCKHTABLE

  HASHCTL info;
  // HTAB *lockhtab;
  HASH_SEQ_STATUS status;
  
  // perhaps there is an optimization here
  info.keysize = sizeof(LOCKTAG);
  info.entrysize = sizeof(LockTableEntry);

  // LOCKHTABLE = hash_create("AcceleratedSnapshotIsolationLocksHashTable", 
  // 1024,
  // &hash_ctl,
  // HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
  LOCKHTABLE = ShmemInitHash("AcceleratedSnapshotIsolationLocksHashTable",
                              1 << 11,
                              1 << 12,
                              &info,
                              HASH_ELEM | HASH_BLOBS);
  /*not concerned about hash_destroy just yet*/
  ereport(LOG, errmsg_internal("created lock hash table"));
}


/* 
* This implementation is straight up taken from src/backend/utils/adt/misc.c
* since I couldn't figure out how to call that function from here
*/
void sleep(float8 secs)
{
	float8		endtime;

	/*
	 * We sleep using WaitLatch, to ensure that we'll wake up promptly if an
	 * important signal (such as SIGALRM or SIGINT) arrives.  Because
	 * WaitLatch's upper limit of delay is INT_MAX milliseconds, and the user
	 * might ask for more than that, we sleep for at most 10 minutes and then
	 * loop.
	 *
	 * By computing the intended stop time initially, we avoid accumulation of
	 * extra delay across multiple sleeps.  This also ensures we won't delay
	 * less than the specified time when WaitLatch is terminated early by a
	 * non-query-canceling signal such as SIGHUP.
	 */
#define GetNowFloat()	((float8) GetCurrentTimestamp() / 1000000.0)
      ereport(LOG, errmsg_internal("sleeping"));
	endtime = GetNowFloat() + secs;

	for (;;)
	{
		float8 delay;
		long delay_ms;

		CHECK_FOR_INTERRUPTS();

		delay = endtime - GetNowFloat();
		if (delay >= 600.0)
			delay_ms = 600000;
		else if (delay > 0.0)
			delay_ms = (long) ceil(delay * 1000.0);
		else
			break;

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 delay_ms,
						 WAIT_EVENT_PG_SLEEP);
		ResetLatch(MyLatch);
	}
      ereport(LOG, errmsg_internal("sleep done"));
}

/*
* 
*/
Datum
asi_try_exclusive_lock(PG_FUNCTION_ARGS)
{
  int32 tableKey = PG_GETARG_INT32(0);
  int32 rowKey = PG_GETARG_INT32(1);
  // For table level lock
  LOCKTAG tbltag;
  LockAcquireResult tblresult;
  // For row level lock
  LOCKTAG tag;
  LockAcquireResult res;

  // max number of retries to attempt to get the lock
  int retries = 1;  
  // timeout between retries (ms)
  float8 timeout = TIMEOUT_MS / 1000;

  // for keeping track of exclusive locks on entries
  LockTableEntry* lte;

  SET_LOCKTAG(tbltag, tableKey, 0);
  SET_LOCKTAG(tag, tableKey, rowKey);

  for(;;)
  {
    // try getting lock on the table
    tblresult = LockAcquire(&tbltag, RowShareLock, false, true);
    if (tblresult != LOCKACQUIRE_NOT_AVAIL)
    {
      // if lock is obtained on the table, then try getting a lock on the row
      res = LockAcquire(&tag, ExclusiveLock, false, true);
      if(res != LOCKACQUIRE_NOT_AVAIL)
      {
        ereport(LOG, errmsg_internal("obtained exclusive lock using tuple (%d, %d)", tableKey, rowKey));

        // set nlocks to -1 to indicate exclusive lock
        // we can do this safely since either the entry will not exist
        // or nlocks >= 0
        // nlocks can never be -1 since otherwise the ExclusiveLock would not have been granted in the first place
        lte = (LockTableEntry *)hash_search(LOCKHTABLE, &tag, HASH_ENTER, NULL);
        ereport(LOG, errmsg_internal("inserted value into lock table"));
        lte->nlocks = -1;
        ereport(LOG, errmsg_internal("inserted: tag: (%d, %d), nlocks: %d", tag.locktag_field2, tag.locktag_field3, (lte == NULL ? -2 : lte->nlocks)));
        PG_RETURN_BOOL(1);
      }
      else
      {
        (void) LockRelease(&tbltag, RowShareLock, false);
      }
    }
    // locks could not be acquired, retry if possible
    if (retries < 3)
    {
      retries++;
      // sleep for `timeout` seconds before retrying
      sleep(timeout);
    }
    else
    {
      // report an error so that the current transaction is aborted
      ereport(ERROR, errmsg_internal("could not obtain lock"));
      PG_RETURN_BOOL(0);
    }
  }
}

Datum
asi_try_shared_lock(PG_FUNCTION_ARGS)
{
  int32 tableKey = PG_GETARG_INT32(0);
  int32 rowKey = PG_GETARG_INT32(1);
  // For table level lock
  LOCKTAG tbltag;
  LockAcquireResult tblresult;
  // For row level lock
  LOCKTAG tag;
  LockAcquireResult res = LOCKACQUIRE_NOT_AVAIL;

  // max number of retries to attempt to get the lock
  int retries = 1;  
  // timeout between retries (ms)
  float8 timeout = TIMEOUT_MS / 1000;

  SET_LOCKTAG(tbltag, tableKey, 0);
  SET_LOCKTAG(tag, tableKey, rowKey);

  for(;;)
  {
    // try getting lock on the table
    tblresult = LockAcquire(&tbltag, RowShareLock, false, true);
    if (tblresult != LOCKACQUIRE_NOT_AVAIL)
    {
      // if lock is obtained on the table, then try getting a lock on the row
      res = LockAcquire(&tag, ShareLock, false, true);
      if(res != LOCKACQUIRE_NOT_AVAIL)
      {
        ereport(LOG, errmsg_internal("obtained shared lock using tuple (%d, %d)", tableKey, rowKey));
        PG_RETURN_BOOL(1);
      }
      else
      {
        (void) LockRelease(&tbltag, AccessShareLock, false);
      }
    }

    // locks could not be acquired, retry if possible
    if (retries < MAX_RETRIES)
    {
      retries++;
      // sleep for `timeout` seconds before retrying
      sleep(timeout);
    }
    else 
    {
      // report an error so that the current transaction is aborted
      ereport(ERROR, errmsg_internal("could not obtain lock"));
      PG_RETURN_BOOL(0);
    }
  }
}

Datum
asi_try_intent_lock(PG_FUNCTION_ARGS)
{
  int32 tableKey = PG_GETARG_INT32(0);
  int32 rowKey = PG_GETARG_INT32(1);
  // For table level lock
  LOCKTAG tbltag;
  LockAcquireResult tblresult;
  // For row level lock
  LOCKTAG tag;
  LockAcquireResult res;

  // max number of retries to attempt to get the lock
  int retries = 1;  
  // timeout between retries (ms)
  float8 timeout = TIMEOUT_MS / 1000;
  // hash table stuff
  bool found;
  LockTableEntry* lte;

  SET_LOCKTAG(tbltag, tableKey, 0);
  SET_LOCKTAG(tag, tableKey, rowKey);

  for(;;)
  {
    // first check if someone has an exclusive lock on this item
    lte = (LockTableEntry *) hash_search(LOCKHTABLE, &tag, HASH_ENTER, &found);
    // if there is no entry or if no one holds an exclusive lock on it
    // then increment nlocks
    if(!found)
    {
      // set nlocks since it may have some garbage value
      lte->nlocks = 0;
    }
    ereport(LOG, errmsg_internal("search: found: %d tag: (%d, %d), nlocks: %d", found, tag.locktag_field2, tag.locktag_field3, (lte == NULL ? -2 : lte->nlocks)));
    if(lte->nlocks >= 0)
    {
      lte->nlocks++;
      // try getting lock on the table
      tblresult = LockAcquire(&tbltag, AccessShareLock, false, true);
      if (tblresult != LOCKACQUIRE_NOT_AVAIL)
      {
        // if lock is obtained on the table, then try getting a lock on the row
        res = LockAcquire(&tag, AccessShareLock, false, true);
        if(res != LOCKACQUIRE_NOT_AVAIL)
        {
          ereport(LOG, errmsg_internal("obtained intent lock using tuple (%d, %d)", tableKey, rowKey));
          PG_RETURN_BOOL(1);
        }
        else 
        {
          // if lock could not be obtained on the tuple, then release the table lock
          (void) LockRelease(&tbltag, AccessShareLock, false);
        }
      }
      else {
      // if either table or row lock could not be acquired then decrement nlocks
        lte->nlocks--;
      }
    }

    // locks could not be acquired, retry if possible
    if (retries < 3)
    {
      retries++;
      // sleep for `timeout` seconds before retrying
      sleep(timeout);
    }
    else {
      // report an error so that the current transaction is aborted
      ereport(ERROR, errmsg_internal("could not obtain lock"));
      PG_RETURN_BOOL(0);
    }
  }
}

Datum
asi_try_table_exclusive_lock(PG_FUNCTION_ARGS)
{
  int32 tableKey = PG_GETARG_INT32(0);
  LOCKTAG tag;
  LockAcquireResult res;

  // max number of retries to attempt to get the lock
  int retries = 1;  
  // timeout between retries (ms)
  float8 timeout = TIMEOUT_MS / 1000;

  SET_LOCKTAG(tag, tableKey, 0);

  for(;;)
  {
    res = LockAcquire(&tag, ExclusiveLock, false, true);
    ereport(LOG, errmsg_internal("res: %d", res));
    if(res != LOCKACQUIRE_NOT_AVAIL){
      ereport(LOG, errmsg_internal("obtained exclusive table lock using tuple (%d, 0)", tableKey));
      PG_RETURN_BOOL(1);
    }
    else if (retries < 3){
      retries++;
      // sleep for `timeout` seconds before retrying
      sleep(timeout);
    }
    else {
      ereport(ERROR, errmsg_internal("could not obtain lock"));
      PG_RETURN_BOOL(0);
    }
  }
}

// For debugging purposes only -- don't use this in actual queries!
Datum
asi_release_intent_lock(PG_FUNCTION_ARGS)
{
  int32 tableKey = PG_GETARG_INT32(0);
  int32 rowKey = PG_GETARG_INT32(1);
  LOCKTAG tag;
  SET_LOCKTAG(tag, tableKey, rowKey);

  release_intent_locks(&tag);
  PG_RETURN_VOID();
}

// validates intention locks before releasing them
// this should be called at the end of a short transaction
Datum
asi_validate_intent_lock(PG_FUNCTION_ARGS)
{
  int32 tableKey = PG_GETARG_INT32(0);
  int32 rowKey = PG_GETARG_INT32(1);
  //
  LOCKTAG tag;
  //
  bool found;
  LockTableEntry *lte;

  SET_LOCKTAG(tag, tableKey, rowKey);

  // search for lte entry
  lte = (LockTableEntry *) hash_search(LOCKHTABLE, &tag, HASH_FIND, &found);
  // it should always exist, even if the short transaction is to be aborted
  Assert(found);
  ereport(LOG, errmsg_internal("validate: found: %d tag: (%d, %d), nlocks: %d", found, tag.locktag_field2, tag.locktag_field3, (lte == NULL ? -2 : lte->nlocks)));
  if(lte->nlocks <= 0)
  {
    // either an exclusive lock is held or was held during the time
    // the short txn finished running so abort this transaction
    // this is done here by throwing an error
    ereport(ERROR, errmsg_internal("intent locks invalidated"));
    PG_RETURN_BOOL(0);
  }
  lte->nlocks--;

  PG_RETURN_BOOL(1);
}

inline static void
LOCK_PRINT(const char *where, const LOCK *lock, LOCKMODE type)
{
		elog(LOG,
			 "%s: lock(%p) id(%u,%u,%u,%u,%u,%u) grantMask(%x) "
			 "req(%d,%d,%d,%d,%d,%d,%d)=%d "
			 "grant(%d,%d,%d,%d,%d,%d,%d)=%d wait(%d)",
			 where, lock,
			 lock->tag.locktag_field1, lock->tag.locktag_field2,
			 lock->tag.locktag_field3, lock->tag.locktag_field4,
			 lock->tag.locktag_type, lock->tag.locktag_lockmethodid,
			 lock->grantMask,
			 lock->requested[1], lock->requested[2], lock->requested[3],
			 lock->requested[4], lock->requested[5], lock->requested[6],
			 lock->requested[7], lock->nRequested,
			 lock->granted[1], lock->granted[2], lock->granted[3],
			 lock->granted[4], lock->granted[5], lock->granted[6],
			 lock->granted[7], lock->nGranted,
			 dclist_count(&lock->waitProcs));
}

// releases exclusive locks in the hash table
// the lock itself is released when the txn commits/aborts
// this function just increments the LockTableEntry
// intended to be called at the end of a long transaction
Datum
asi_release_exclusive_lock(PG_FUNCTION_ARGS)
{
  int32 tableKey = PG_GETARG_INT32(0);
  int32 rowKey = PG_GETARG_INT32(0);
  //
  LOCKTAG tag;
  //
  bool found;
  LockTableEntry* lte;

  SET_LOCKTAG(tag, tableKey, rowKey);

  // search for the lte entry
  // it should always exist
  lte = (LockTableEntry *)hash_search(LOCKHTABLE, &tag, HASH_FIND, &found);

  // can set it to zero safely, if found
  if(found) {
    lte->nlocks = 0;
  }
  ereport(LOG, errmsg_internal("found: %d tag: (%d, %d), nlocks: %d", found, tag.locktag_field2, tag.locktag_field3, (lte == NULL ? -2 : lte->nlocks)));

  PG_RETURN_VOID();
}

static void release_intent_locks(LOCKTAG* locktag){
  // Obtain partition lock on the shared hash table in the lockmgr
  // Compute hash code
  // LOCK *lock;
  // uint32 hashcode;
  // LWLock *partitionLock;

  // hashcode = LockTagHashCode(locktag);
  // partitionLock = LockHashPartitionLock(hashcode);

  // LWLockAcquire(partitionLock, LW_EXCLUSIVE);

  // lock = (LOCK *) hash_search_with_hash_value(LockMethodLockHash, locktag, hashcode, HASH_FIND, NULL);

  // LWLockRelease(partitionLock);

  /*
  * what needs to be done instead is the following:
  * any exclusive lock acquisition should invalidate the intent lock entry
  * on intent lock validation, the bit is reset
  * this needs to be a fancy lookup table: can use hash<->bit to store this
  * maybe even use my own local hash table while I'm at it?
  * would require the following changes:
  *   - init routine to set up the hash table
  *   - exclusive lock acquisition inserts value into hash table
  *   - intent lock acquisition and validation checks this value for all intent locks before returning
  *   - exclusive lock release clears this value
  *   - this may be a problem if the txn is aborted (TBD)
  *   - no wait no that is a problem, coz my thing includes aborts
  *   - maybe expose a function that clears the key-value pair, outside the transaction
  */
}