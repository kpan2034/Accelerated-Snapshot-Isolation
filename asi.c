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
#include "storage/proc.h"
#include "storage/latch.h"
#include "pgstat.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "lib/ilist.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(asi_try_exclusive_lock);
PG_FUNCTION_INFO_V1(asi_try_shared_lock);
PG_FUNCTION_INFO_V1(asi_try_intent_lock);
// PG_FUNCTION_INFO_V1(asi_try_table_exclusive_lock);
// PG_FUNCTION_INFO_V1(asi_try_table_shared_lock);
PG_FUNCTION_INFO_V1(asi_validate_intent_lock);
// PG_FUNCTION_INFO_V1(asi_release_intent_lock);
PG_FUNCTION_INFO_V1(asi_release_exclusive_lock);
PG_FUNCTION_INFO_V1(asi_release_shared_lock);
PG_FUNCTION_INFO_V1(asi_intent_lock);

// don't need these since the above functions all acquire transaction locks
// PG_FUNCTION_INFO_V1(asi_short_unlock);
// PG_FUNCTION_INFO_V1(asi_long_unlock);

void sleep(float8 secs);
static void release_intent_locks(LOCKTAG *locktag);

#define SET_LOCKTAG(tag, key1, key2) \
  SET_LOCKTAG_ADVISORY(tag, MyDatabaseId, key1, key2, 2)
#define MAX_RETRIES 3
#define TIMEOUT_MS 15.0
static HTAB *LOCKHTABLE;

//extern PGPROC* MyProc;
// extern PROC_HDR* ProcGlobal;

typedef struct ProcId
{
  dlist_node links;
  int procno;
} ProcId;

typedef struct LockTableEntry
{
  // store the tag value as well
  LOCKTAG tag;
  // indicates number of AccessShareLocks on this table entry
  int nlocks;
  // list of procids that are waiting for this lock
  // type: int
  dclist_head waitProcs;
} LockTableEntry;

#define _DEBUG
#ifdef _DEBUG
static inline void DEBUG_ILIST(dclist_head * head, const char * errMsg)
{
  dlist_iter _iter;
  ereport(LOG, errmsg_internal("%p has size %d", head, dclist_count(head)));
  dclist_foreach(_iter, head) 
  { 
    ProcId *ptr = dclist_container(ProcId, links, _iter.cur);
    ereport(LOG, errmsg_internal("%s: process %d", errMsg, ptr->procno)); 
  } 
}
#else
#define DEBUG_ILIST(x, y) do { } while(0)
#endif


void _PG_init(void)
{
  // You probably need to figure out how to put aside some shared memory for your hash table here

  /* Set up hash table here */

  /* Can use GUC here to setup timeouts, etc */
  // DefineCustomIntVariable();

  // Unlike the LockMethodLockHash used in lock.c
  // here we expect that each LOCKTAG has only a single entry
  // in LOCKHTABLE
  if (LOCKHTABLE == NULL)
  {
    HASHCTL info;
    // HTAB *lockhtab;
    // HASH_SEQ_STATUS status;

    // perhaps there is an optimization here
    info.keysize = sizeof(LOCKTAG);
    info.entrysize = sizeof(LockTableEntry);

    // LOCKHTABLE = hash_create("AcceleratedSnapshotIsolationLocksHashTable",
    // 1024,
    // &hash_ctl,
    // HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    // // ereport(LOG, errmsg_internal("%s: Acquired LW_EXCLUSIVE lock.", "PG_INIT"));
    LWLockAcquire(WriteGuardTableLock, LW_EXCLUSIVE);
    LOCKHTABLE = ShmemInitHash("AcceleratedSnapshotIsolationLocksHashTable",
                              1 << 11,
                              1 << 12,
                              &info,
                              HASH_ELEM | HASH_BLOBS);
    /*not concerned about hash_destroy just yet*/
// // ereport(LOG, errmsg_internal("%s: Released LW_EXCLUSIVE lock.", "PG_INIT"));
    LWLockRelease(WriteGuardTableLock);
    // // ereport(LOG, errmsg_internal("created lock hash table: %p", LOCKHTABLE));
  }
}

/*
 * This implementation is straight up taken from src/backend/utils/adt/misc.c
 * since I couldn't figure out how to call that function from here
 */
void sleep(float8 secs)
{
  float8 endtime;

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
#define GetNowFloat() ((float8)GetCurrentTimestamp() / 1000000.0)
  // // ereport(LOG, errmsg_internal("sleeping"));
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
      delay_ms = (long)ceil(delay * 1000.0);
    else
      break;

    (void)WaitLatch(MyLatch,
                    WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                    delay_ms,
                    WAIT_EVENT_PG_SLEEP);
    ResetLatch(MyLatch);
  }
  // // ereport(LOG, errmsg_internal("sleep done"));
}

/*
 *
 */
Datum asi_try_exclusive_lock(PG_FUNCTION_ARGS)
{
  // reuse these for table locks, too? probably optimize later
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
  LockTableEntry *tbllte;
  LockTableEntry *rowlte;

  // testing stuff
  // LockTableEntry *testlte;
  bool found;

  SET_LOCKTAG(tbltag, tableKey, 0);
  SET_LOCKTAG(tag, tableKey, rowKey);

  for (;;)
  {
    // Also insert lte for the table
  // // ereport(LOG, errmsg_internal("%s: Acquired LW_EXCLUSIVE lock.", "exclusive_lock"));
    // try getting lock on the table
    ereport(LOG, errmsg_internal("attempting to get lock on table: (%d, %d)", tableKey, 0));
    tblresult = LockAcquire(&tbltag, RowShareLock, true, false);
    if (tblresult != LOCKACQUIRE_NOT_AVAIL)
    {
      // if lock is obtained on the table, then try getting a lock on the row
      ereport(LOG, errmsg_internal("attempting to get lock on row: (%d, %d)", tableKey, rowKey));
      res = LockAcquire(&tag, ExclusiveLock, true, false);
      if (res != LOCKACQUIRE_NOT_AVAIL)
      {
        ereport(LOG, errmsg_internal("obtained exclusive lock using tuple (%d, %d)", tableKey, rowKey));
        LWLockAcquire(WriteGuardTableLock, LW_EXCLUSIVE);
        tbllte = (LockTableEntry *)hash_search(LOCKHTABLE, &tbltag, HASH_ENTER, &found);
        tbllte->nlocks = -1;
        // lte->tag = &tbltag;
        if(!found)
          dclist_init(&tbllte->waitProcs);
        // LWLockRelease(WriteGuardTableLock);

        // // ereport(LOG, errmsg_internal("inserted in: %p", LOCKHTABLE));
        // // ereport(LOG, errmsg_internal("inserted: tag: (%d, %d), nlocks: %d", tbltag.locktag_field2, tbltag.locktag_field3, (tbllte == NULL ? -2 : tbllte->nlocks)));

        // testlte = (LockTableEntry *)hash_search(LOCKHTABLE, &tbltag, HASH_FIND, &found);
        // if (!found) {
        //   ereport(ERROR, errmsg_internal("wth"));
        // }
        // // ereport(LOG, errmsg_internal("okay was able to search for something here"), LOCKHTABLE);
        // // ereport(LOG, errmsg_internal("found: %d tag: (%d, %d), nlocks: %d", found, tbltag.locktag_field2, tbltag.locktag_field3, (testlte == NULL ? -2 : testlte->nlocks)));
        // Assert(found && testlte->nlocks == -1);

        // set nlocks to -1 to indicate exclusive lock
        // we can do this safely since either the entry will not exist
        // or nlocks >= 0
        // nlocks can never be -1 since otherwise the ExclusiveLock would not have been granted in the first place
        // LWLockAcquire(WriteGuardTableLock, LW_EXCLUSIVE);
        rowlte = (LockTableEntry *)hash_search(LOCKHTABLE, &tag, HASH_ENTER, &found);
        rowlte->nlocks = -1;
        // lte->tag = &tag;
        if(!found)
          dclist_init(&rowlte->waitProcs);
        // // ereport(LOG, errmsg_internal("inserted: tag: (%d, %d), nlocks: %d", tag.locktag_field2, tag.locktag_field3, (rowlte == NULL ? -2 : rowlte->nlocks)));
// // ereport(LOG, errmsg_internal("%s: Released LW_EXCLUSIVE lock.", "exclusive_lock"));
        LWLockRelease(WriteGuardTableLock);

        PG_RETURN_BOOL(1);
      }
      else
      {
// // ereport(LOG, errmsg_internal("%s: Released LW_EXCLUSIVE lock.", "exclusive_lock"));
      ereport(LOG, errmsg_internal("attempting to release lock on table: (%d, %d)", tableKey, 0));
        (void)LockRelease(&tbltag, RowShareLock, true);
      }
    }

    // locks could not be acquired, retry if possible
    if (retries < 3)
    {
// // ereport(LOG, errmsg_internal("%s: Released LW_EXCLUSIVE lock.", "exclusive_lock"));
      retries++;
      // sleep for `timeout` seconds before retrying
      sleep(timeout);
    }
    else
    {
// // ereport(LOG, errmsg_internal("%s: Released LW_EXCLUSIVE lock.", "exclusive_lock"));
      // report an error so that the current transaction is aborted
      ereport(ERROR, errmsg_internal("could not obtain lock"));
      PG_RETURN_BOOL(0);
    }
  }
}

Datum asi_try_shared_lock(PG_FUNCTION_ARGS)
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
  LockTableEntry *lte;
  bool found;

  SET_LOCKTAG(tbltag, tableKey, 0);
  SET_LOCKTAG(tag, tableKey, rowKey);

  for (;;)
  {
    // check if table has some lock on it first
    // LWLockAcquire(WriteGuardTableLock, LW_SHARED);
    // lte = (LockTableEntry *)hash_search(LOCKHTABLE, &tbltag, HASH_FIND, &found);
    // LWLockRelease(WriteGuardTableLock);
    // // ereport(LOG, errmsg_internal("found: tag: (%d, %d), nlocks: %d", tbltag.locktag_field2, tbltag.locktag_field3, (lte == NULL ? -2 : lte->nlocks)));
    // if (!found || lte->nlocks >= 0)
    if (true)
    {
      // can attempt to get lock on row
      // try getting lock on the table
      ereport(LOG, errmsg_internal("attempting to get lock on table: (%d, %d)", tableKey, 0));
      tblresult = LockAcquire(&tbltag, RowShareLock, true, false);
      if (tblresult != LOCKACQUIRE_NOT_AVAIL)
      {
        // if lock is obtained on the table, then try getting a lock on the row
        ereport(LOG, errmsg_internal("attempting to get lock on row: (%d, %d)", tableKey, rowKey));
        res = LockAcquire(&tag, ShareLock, true, false);
        if (res != LOCKACQUIRE_NOT_AVAIL)
        {
          // // ereport(LOG, errmsg_internal("obtained shared lock using tuple (%d, %d)", tableKey, rowKey));
          PG_RETURN_BOOL(1);
        }
        else
        {
          ereport(LOG, errmsg_internal("attempting to release lock on table: (%d, %d)", tableKey, 0));
          (void)LockRelease(&tbltag, RowShareLock, true);
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
    else
    {
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
}

Datum asi_try_intent_lock(PG_FUNCTION_ARGS)
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
  LockTableEntry *tbllte;
  LockTableEntry *rowlte;

  SET_LOCKTAG(tbltag, tableKey, 0);
  SET_LOCKTAG(tag, tableKey, rowKey);

  for (;;)
  {
    // first check if someone has an exclusive lock on the table
    LWLockAcquire(WriteGuardTableLock, LW_EXCLUSIVE);
    tbllte = (LockTableEntry *)hash_search(LOCKHTABLE, &tbltag, HASH_ENTER, &found);
    if(!found)
    {
      // tbllte->tag = &tbltag;
      tbllte->nlocks = 0;
      // // ereport(LOG, errmsg_internal("Init'ing tbl->waitprocs"));
      dclist_init(&tbllte->waitProcs);
    }
    LWLockRelease(WriteGuardTableLock);
    // // ereport(LOG, errmsg_internal("search: found: %d tag: (%d, %d), nlocks: %d", found, tbltag.locktag_field2, tbltag.locktag_field3, (tbllte == NULL ? -2 : tbllte->nlocks)));
    if (tbllte->nlocks >= 0)
    {
      // check if someone has an exclusive lock on this item
      LWLockAcquire(WriteGuardTableLock, LW_EXCLUSIVE);
      rowlte = (LockTableEntry *)hash_search(LOCKHTABLE, &tag, HASH_ENTER, &found);
      // if there is no entry or if no one holds an exclusive lock on it
      // then increment nlocks
      if (!found)
      {
        // set nlocks since it may have some garbage value
        // rowlte->tag = &tag;
        rowlte->nlocks = 0;
      // // ereport(LOG, errmsg_internal("Init'ing row->waitprocs"));
        dclist_init(&rowlte->waitProcs);
      }
      LWLockRelease(WriteGuardTableLock);
      // // ereport(LOG, errmsg_internal("search: found: %d tag: (%d, %d), nlocks: %d", found, tag.locktag_field2, tag.locktag_field3, (rowlte == NULL ? -2 : rowlte->nlocks)));
      if (rowlte->nlocks >= 0)
      {
        // try getting lock on the table
        // Note: This should always be granted since AccessShareLock doesn't conflict with
        // the other three lock modes that we are using (ShareLock, ExclusiveLock, RowShareLock);
        // we do this for posterity's sake
        tblresult = LockAcquire(&tbltag, AccessShareLock, true, true);
        if (tblresult != LOCKACQUIRE_NOT_AVAIL)
        {
          // if lock is obtained on the table, then try getting a lock on the row
          res = LockAcquire(&tag, AccessShareLock, true, true);
          if (res != LOCKACQUIRE_NOT_AVAIL)
          {
            // // ereport(LOG, errmsg_internal("obtained intent lock using tuple (%d, %d)", tableKey, rowKey));
            // all good so increment nlocks
            tbllte->nlocks++;
            rowlte->nlocks++;
            PG_RETURN_BOOL(1);
          }
          else
          {
            // if lock could not be obtained on the tuple, then release the table lock
            (void)LockRelease(&tbltag, AccessShareLock, true);
          }
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
    else if (retries < 3)
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

Datum asi_try_table_exclusive_lock(PG_FUNCTION_ARGS)
{
  int32 tableKey = PG_GETARG_INT32(0);
  LOCKTAG tag;
  LockAcquireResult res;

  // max number of retries to attempt to get the lock
  int retries = 1;
  // timeout between retries (ms)
  float8 timeout = TIMEOUT_MS / 1000;

  SET_LOCKTAG(tag, tableKey, 0);

  for (;;)
  {
    res = LockAcquire(&tag, ExclusiveLock, true, true);
    // // ereport(LOG, errmsg_internal("res: %d", res));
    if (res != LOCKACQUIRE_NOT_AVAIL)
    {
      // // ereport(LOG, errmsg_internal("obtained exclusive table lock using tuple (%d, 0)", tableKey));
      PG_RETURN_BOOL(1);
    }
    else if (retries < 3)
    {
      retries++;
      // sleep for `timeout` seconds before retrying
      sleep(timeout);
    }
    else
    {
      ereport(ERROR, errmsg_internal("could not obtain lock"));
      PG_RETURN_BOOL(0);
    }
  }
}

// For debugging purposes only -- don't use this in actual queries!
Datum asi_release_intent_lock(PG_FUNCTION_ARGS)
{
  int32 tableKey = PG_GETARG_INT32(0);
  int32 rowKey = PG_GETARG_INT32(1);
  LOCKTAG tag;
  SET_LOCKTAG(tag, tableKey, rowKey);

  release_intent_locks(&tag);
  PG_RETURN_VOID();
}

// validates and (releases!) intention locks
// this should be called at the end of a short transaction
Datum asi_validate_intent_lock(PG_FUNCTION_ARGS)
{
  int32 tableKey = PG_GETARG_INT32(0);
  int32 rowKey = PG_GETARG_INT32(1);
  // For table level lock
  LOCKTAG tbltag;
  // For row level lock
  LOCKTAG tag;
  // hash table stuff
  bool found;
  LockTableEntry *rowlte;
  LockTableEntry *tbllte;
  // are the locks still valid?
  bool valid = true;

  SET_LOCKTAG(tbltag, tableKey, 0);
  SET_LOCKTAG(tag, tableKey, rowKey);

  // search for lte row entry
  // // ereport(LOG, errmsg_internal("%s: Acquired LW_EXCLUSIVE lock.", "validate_intent_lock"));
  LWLockAcquire(WriteGuardTableLock, LW_EXCLUSIVE);
  rowlte = (LockTableEntry *)hash_search(LOCKHTABLE, &tag, HASH_FIND, &found);
  // // ereport(LOG, errmsg_internal("validate: found: %d tag: (%d, %d), nlocks: %d", found, tag.locktag_field2, tag.locktag_field3, (rowlte == NULL ? -2 : rowlte->nlocks)));
  // it should always exist, even if the short transaction is to be aborted
  Assert(found);
  if (rowlte->nlocks <= 0)
  {
    valid = false;
    // either an exclusive lock is held or was held during the time
    // the short txn finished running so abort this transaction
    // this is done here by throwing an error
    // if nlocks is zero, this means that some transaction took up a write lock
    // and then released it
    // so we can safely remove that entry from the hash table
    // if (rowlte->nlocks == 0)
    // {
    // (void) hash_search(LOCKHTABLE, &tag, HASH_REMOVE, NULL);
    // }

    // strictly speaking, we don't _need_ to "release" the AccessShareLock
    // since the conflicts will never happen here
    // but better to
    // (void) LockRelease(&tag, AccessShareLock, true);
    // (void) LockRelease(&tbltag, AccessShareLock, true);

    // ereport(ERROR, errmsg_internal("intent locks invalidated"));
  }
  else
  {
    // we can always do this safely, even if
    rowlte->nlocks--;
  }
  // No more intent writers; can safely remove the entry from the hash table
  if (rowlte->nlocks == 0)
  {
    (void)hash_search(LOCKHTABLE, &tag, HASH_REMOVE, NULL);
  }
  // Also validate the table!

  // search for lte table entry
  tbllte = (LockTableEntry *)hash_search(LOCKHTABLE, &tbltag, HASH_FIND, &found);
  // // ereport(LOG, errmsg_internal("validate: found: %d tag: (%d, %d), nlocks: %d", found, tbltag.locktag_field2, tbltag.locktag_field3, (tbllte == NULL ? -2 : tbllte->nlocks)));
  if (tbllte->nlocks <= 0)
  {
    valid = false;
    // either an exclusive lock is held or was held during the time
    // the short txn finished running so abort this transaction
    // this is done here by throwing an error
    // if (tbllte->nlocks == 0)
    // {
    // (void) hash_search(LOCKHTABLE, &tbltag, HASH_REMOVE, NULL);
    // }
    // (void) LockRelease(&tag, AccessShareLock, true);
    // (void) LockRelease(&tbltag, AccessShareLock, true);
    // ereport(ERROR, errmsg_internal("intent locks invalidated"));
  }
  tbllte->nlocks--;
  if (tbllte->nlocks == 0)
  {
    (void)hash_search(LOCKHTABLE, &tbltag, HASH_REMOVE, NULL);
  }
// // ereport(LOG, errmsg_internal("%s: Released LW_EXCLUSIVE lock.", "validate_intent_lock"));
  LWLockRelease(WriteGuardTableLock);

  // (void)LockRelease(&tag, AccessShareLock, true);
  // (void)LockRelease(&tbltag, AccessShareLock, true);

  if (!valid)
  {
    ereport(ERROR, errmsg_internal("intent lock invalid"));
  }

  PG_RETURN_VOID();
}

// releases exclusive locks in the hash table
// the lock itself is released when the txn commits/aborts
// this function just increments the LockTableEntry
// intended to be called at the end of a long transaction
Datum asi_release_exclusive_lock(PG_FUNCTION_ARGS)
{
  int32 tableKey = PG_GETARG_INT32(0);
  int32 rowKey = PG_GETARG_INT32(1);
  // For table level lock
  LOCKTAG tbltag;
  // For row level lock
  LOCKTAG tag;
  // hash table stuff
  bool found;
  LockTableEntry *rowlte;
  LockTableEntry *tbllte;
  // Signalling stuff
  dlist_mutable_iter iter;

  SET_LOCKTAG(tbltag, tableKey, 0);
  SET_LOCKTAG(tag, tableKey, rowKey);


  // search for the lte entry
  // it should always exist
  LWLockAcquire(WriteGuardTableLock, LW_EXCLUSIVE);
// ereport(LOG, errmsg_internal("%s: Acquired LW_EXCLUSIVE lock.", "release_exclusive_lock"));
  rowlte = (LockTableEntry *)hash_search(LOCKHTABLE, &tag, HASH_FIND, &found);
// ereport(LOG, errmsg_internal("found: %d tag: (%d, %d), nlocks: %d", found, tag.locktag_field2, tag.locktag_field3, (rowlte == NULL ? -2 : rowlte->nlocks)));
  // can set it to zero safely, if found -- should always be the case
  Assert(found && rowlte->nlocks <= 0);
  rowlte->nlocks = 0;

  // reset the table entry too
  tbllte = (LockTableEntry *)hash_search(LOCKHTABLE, &tbltag, HASH_FIND, &found);
  // // ereport(LOG, errmsg_internal("found: %d tag: (%d, %d), nlocks: %d", found, tbltag.locktag_field2, tbltag.locktag_field3, (tbllte == NULL ? -2 : tbllte->nlocks)));

  // DEBUG_ILIST(&tbllte->waitProcs, "RIGHT AFTER FINDING IT");
  // can set it to zero safely, if found -- should always be the case
  Assert(found && tbllte->nlocks <= 0);
  tbllte->nlocks = 0;


  // dclist_foreach(iter, &tbllte->waitProcs)
  // {
    // // ereport(LOG, errmsg_internal("iterating entries: current entry: %p", iter.cur));
    // ProcId *proc = dclist_container(ProcId, links, iter.cur);
    // // ereport(LOG, errmsg_internal("found: %d (%d)", proc, proc->procno));
    // latch = &proc->procLatch;
    // SetLatch(latch);
    // Assert(proc != NULL);
    // // ereport(LOG, errmsg_internal("Sending signal..."));
    // ProcSendSignal(proc->procno);
    // // ereport(LOG, errmsg_internal("Sent signal to: %d", proc->procno));
  // }

  // DEBUG_ILIST(&tbllte->waitProcs, "BEFORE RELEASING LOCKS");
  // first release the lock on the row
  ereport(LOG, errmsg_internal("attempting to release lock on row: (%d, %d)", tableKey, rowKey));
  (void)LockRelease(&tag, ExclusiveLock, true);

  // now release the lock on the table
  ereport(LOG, errmsg_internal("attempting to release lock on table: (%d, %d)", tableKey, 0));
  (void)LockRelease(&tbltag, RowShareLock, true);
  // DEBUG_ILIST(&tbllte->waitProcs, "AFTER RELEASING LOCKS");

  // now notify any waiting processes
  // these will be txns waiting for an intent lock on this row/table
  // this is done by setting the latch
  // MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);
  int count = dclist_count(&tbllte->waitProcs);
  // // ereport(LOG, errmsg_internal("iterating list: %p (size %d)", &tbllte->waitProcs, count));

  dclist_foreach_modify(iter, &tbllte->waitProcs)
  {
    // // ereport(LOG, errmsg_internal("current entry: %p", iter.cur));
    if(iter.cur != NULL)
    {
      ProcId *proc = dclist_container(ProcId, links, iter.cur);
      if(proc != NULL)
      {
        // // ereport(LOG, errmsg_internal("found: %d", proc->procno));
        // latch = &proc->procLatch;
        // SetLatch(latch);
        Assert(proc != NULL);
        // // ereport(LOG, errmsg_internal("Sending signal..."));
        if(proc->procno > 0 && proc->procno < ProcGlobal->allProcCount){
                ProcSendSignal(proc->procno);
                // // ereport(LOG, errmsg_internal("Sent signal to: %d", proc->procno));
        }
        else
        {
          // // ereport(LOG, errmsg_internal("no signal sent: %d", proc->procno));
        }
      }
    }
  }

  dlist_mutable_iter iter2;
  count = dclist_count(&rowlte->waitProcs);
  // // ereport(LOG, errmsg_internal("iterating list: %p (size %d)", &rowlte->waitProcs, count));
  dclist_foreach_modify(iter2, &rowlte->waitProcs)
  {
    // // ereport(LOG, errmsg_internal("current entry: %p", iter2.cur));
    if(iter2.cur != NULL)
    {
      ProcId *proc = dclist_container(ProcId, links, iter2.cur);
      if(proc != NULL)
      {
        // // ereport(LOG, errmsg_internal("found: %d", proc->procno));
        // latch = &proc->procLatch;
        // SetLatch(latch);
        Assert(proc != NULL);
        // // ereport(LOG, errmsg_internal("Sending signal..."));
        if(proc->procno > 0 && proc->procno < ProcGlobal->allProcCount){
                ProcSendSignal(proc->procno);
                // // ereport(LOG, errmsg_internal("Sent signal to: %d", proc->procno));
        }
        else
        {
          // ereport(LOG, errmsg_internal("no signal sent: %d", proc->procno));
        }
      }
    }
  }

  LWLockRelease(WriteGuardTableLock);
// ereport(LOG, errmsg_internal("%s: Released LW_EXCLUSIVE lock.", "release_exclusive_lock"));
  PG_RETURN_VOID();
}

Datum asi_release_shared_lock(PG_FUNCTION_ARGS)
{
  int32 tableKey = PG_GETARG_INT32(0);
  int32 rowKey = PG_GETARG_INT32(1);

  // For table level lock
  LOCKTAG tbltag;
  // For row level lock
  LOCKTAG tag;
  // hash table stuff
  bool found;
  LockTableEntry *lte;

  SET_LOCKTAG(tbltag, tableKey, 0);
  SET_LOCKTAG(tag, tableKey, rowKey);

  // first release the share lock on the row
  ereport(LOG, errmsg_internal("attempting to release lock on row: (%d, %d)", tableKey, rowKey));
  (void)LockRelease(&tag, ShareLock, true);

  // release the RowShareLock lock on the table next
  ereport(LOG, errmsg_internal("attempting to release lock on table: (%d, %d)", tableKey, 0));
  (void)LockRelease(&tbltag, RowShareLock, true);

  PG_RETURN_VOID();
}

Datum
asi_intent_lock(PG_FUNCTION_ARGS)
{
  int32 tableKey = PG_GETARG_INT32(0);
  int32 rowKey = PG_GETARG_INT32(1);
  // For table level lock
  LOCKTAG tbltag;
  LockAcquireResult tblresult;
  // For row level lock
  LOCKTAG tag;
  LockAcquireResult res;
  // hash table stuff
  bool found;
  LockTableEntry *tbllte;
  LockTableEntry *rowlte;
  ProcId *entry;

  SET_LOCKTAG(tbltag, tableKey, 0);
  SET_LOCKTAG(tag, tableKey, rowKey);

  // first check if someone has an exclusive lock on the table
  LWLockAcquire(WriteGuardTableLock, LW_EXCLUSIVE);
// ereport(LOG, errmsg_internal("%s: Acquired LW_EXCLUSIVE lock.", "intent_lock"));
  tbllte = (LockTableEntry *)hash_search(LOCKHTABLE, &tbltag, HASH_ENTER, &found);
  if(!found)
  {
    // tbllte->tag = &tbltag;
    tbllte->nlocks = 0;
    // ereport(LOG, errmsg_internal("Init'ing tbl->waitprocs"));
    dclist_init(&tbllte->waitProcs);
  }
  // ereport(LOG, errmsg_internal("search: found: %d tag: (%d, %d), nlocks: %d", found, tbltag.locktag_field2, tbltag.locktag_field3, (tbllte == NULL ? -2 : tbllte->nlocks)));
  for (;;)
  {

    // CHECK_FOR_INTERRUPTS();

    if(tbllte->nlocks>=0){
      break;
    }
    else
    {
      // someone has an exclusive lock on this table, so wait for them to be done writing
      // Add to wait procs list here 
      bool found;
      // entry = (ProcId*) MemoryContextAllocZero(TopMemoryContext, sizeof(ProcId));
      char entry_name[1024];
      pg_snprintf(entry_name, 1024, "%d(%d/%d)", MyProc->pgprocno, tableKey, 0);
      entry = (ProcId *) ShmemInitStruct(entry_name, sizeof(ProcId), &found);
      if(!found)
      {
// ereport(LOG, errmsg_internal("did not find an existing node, init'ing: (%d, %d)", tableKey, 0));
        dlist_node_init(&entry->links);
      }
      entry->procno = MyProc->pgprocno;

      DEBUG_ILIST(&tbllte->waitProcs, "VALIDATING BEFORE INSERT");

      ereport(LOG, errmsg_internal("attempting to add to waiting procs list: %d(%p): %p", entry->procno, &entry->links, &tbllte->waitProcs));
      dclist_push_tail(&tbllte->waitProcs, &entry->links);

      // // ereport(LOG, errmsg_internal("iterating list: %p", &tbllte->waitProcs));
      // dlist_iter iter;
      // dclist_foreach(iter, &tbllte->waitProcs)
      // {
      //   ProcId *proc = dclist_container(ProcId, links, iter.cur);
      //   // ereport(LOG, errmsg_internal("found: %d", proc->procno));
      // }

      // ereport(LOG, errmsg_internal("added to waiting procs list: %d", entry->procno));

      DEBUG_ILIST(&tbllte->waitProcs, "VALIDATING AFTER INSERT");
      // Now wait for latch to be set
      // (void)WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, -1, WAIT_EVENT_LONG_XACT_DONE);
      // ResetLatch(MyLatch);
      LWLockRelease(WriteGuardTableLock);
// ereport(LOG, errmsg_internal("%s: Released LW_EXCLUSIVE lock.", "intent_lock"));
      ProcWaitForSignal(WAIT_EVENT_LONG_XACT_DONE);
      LWLockAcquire(WriteGuardTableLock, LW_EXCLUSIVE);
// ereport(LOG, errmsg_internal("%s: Acquired LW_EXCLUSIVE lock.", "intent_lock"));
      // First, go get the stuff from shared memory again since this may no longer be valid
      tbllte = (LockTableEntry *)hash_search(LOCKHTABLE, &tbltag, HASH_FIND, &found);
      // It should def exist
      Assert(found);
      pg_snprintf(entry_name, 1024, "%d(%d/%d)", MyProc->pgprocno, tableKey, 0);
      entry = (ProcId *) ShmemInitStruct(entry_name, sizeof(ProcId), &found);
      Assert(found);
      ereport(LOG, errmsg_internal("attempting to delete from waiting procs list: %d(%p), %p", entry->procno, &entry->links, &tbllte->waitProcs));
      DEBUG_ILIST(&tbllte->waitProcs, "VALIDATING BEFORE DELETE");
      dclist_delete_from_thoroughly(&tbllte->waitProcs, &entry->links);
      ereport(LOG, errmsg_internal("deleted from waiting procs list: %d", entry->procno));
      DEBUG_ILIST(&tbllte->waitProcs, "VALIDATING AFTER DELETE");
    }
  }
  tbllte->nlocks++;
  // LWLockRelease(WriteGuardTableLock);

  // now check if someone has an exclusive lock on this item
  // LWLockAcquire(WriteGuardTableLock, LW_EXCLUSIVE);
  rowlte = (LockTableEntry *)hash_search(LOCKHTABLE, &tag, HASH_ENTER, &found);
  // if there is no entry or if no one holds an exclusive lock on it
  // then increment nlocks
  if (!found)
  {
    // set nlocks since it may have some garbage value
    // rowlte->tag = &tag;
    rowlte->nlocks = 0;
    // ereport(LOG, errmsg_internal("Init'ing row->waitprocs"));
    dclist_init(&rowlte->waitProcs);
  }
  // ereport(LOG, errmsg_internal("search: found: %d tag: (%d, %d), nlocks: %d", found, tag.locktag_field2, tag.locktag_field3, (rowlte == NULL ? -2 : rowlte->nlocks)));
  for (;;)
  {

    // CHECK_FOR_INTERRUPTS();

    if(rowlte->nlocks>=0){
      break;
    }
    else
    {
      // someone has an exclusive lock on this table, so wait for them to be done writing
      // Add to wait procs list here 
      bool found;
      // entry = (ProcId*) MemoryContextAllocZero(TopMemoryContext, sizeof(ProcId));
      char entry_name[1024];
      pg_snprintf(entry_name, 1024, "%d(%d/%d)", MyProc->pgprocno, tableKey, rowKey);
      entry = (ProcId *) ShmemInitStruct(entry_name, sizeof(ProcId), &found);
      if(!found)
      {
// ereport(LOG, errmsg_internal("did not find an existing node, init'ing: (%d, %d)", tableKey, rowKey));
        dlist_node_init(&entry->links);
      }
      entry->procno = MyProc->pgprocno;
      ereport(LOG, errmsg_internal("attempting to add to waiting procs list: %d(%p): %p", entry->procno, &entry->links, &rowlte->waitProcs));
      DEBUG_ILIST(&rowlte->waitProcs, "VALIDATING BEFORE INSERT");
      dclist_push_tail(&rowlte->waitProcs, &entry->links);
      DEBUG_ILIST(&rowlte->waitProcs, "VALIDATING AFTER INSERT");

      // // ereport(LOG, errmsg_internal("iterating list: %p", &rowlte->waitProcs));
      // dlist_iter iter;
      // dclist_foreach(iter, &rowlte->waitProcs)
      // {
      //   ProcId *proc = dclist_container(ProcId, links, iter.cur);
      //   // ereport(LOG, errmsg_internal("found: %d", proc->procno));
      // }

      // ereport(LOG, errmsg_internal("added to waiting procs list: %d", entry->procno));
      LWLockRelease(WriteGuardTableLock);
ereport(LOG, errmsg_internal("%s: Released LW_EXCLUSIVE lock.", "intent_lock"));
      ProcWaitForSignal(WAIT_EVENT_LONG_XACT_DONE);
      LWLockAcquire(WriteGuardTableLock, LW_EXCLUSIVE);
ereport(LOG, errmsg_internal("%s: Acquired LW_EXCLUSIVE lock.", "intent_lock"));
      // First, go get the stuff from shared memory again since this may no longer be valid
      rowlte = (LockTableEntry *)hash_search(LOCKHTABLE, &tag, HASH_FIND, &found);
      // It should def exist
      Assert(found);
      pg_snprintf(entry_name, 1024, "%d(%d/%d)", MyProc->pgprocno, tableKey, rowKey);
      entry = (ProcId *) ShmemInitStruct(entry_name, sizeof(ProcId), &found);
      Assert(found);
      ereport(LOG, errmsg_internal("attempting to delete from waiting procs list: %d(%p), %p", entry->procno, &entry->links, &rowlte->waitProcs));
      DEBUG_ILIST(&rowlte->waitProcs, "VALIDATING BEFORE DELETE");
      dclist_delete_from_thoroughly(&rowlte->waitProcs, &entry->links);
      ereport(LOG, errmsg_internal("deleted from waiting procs list: %d", entry->procno));
      DEBUG_ILIST(&rowlte->waitProcs, "VALIDATING AFTER DELETE");
    }
  }
  rowlte->nlocks++;
  LWLockRelease(WriteGuardTableLock);
ereport(LOG, errmsg_internal("%s: Released LW_EXCLUSIVE lock.", "intent_lock"));
  PG_RETURN_BOOL(1);
}

static void release_intent_locks(LOCKTAG *locktag)
{
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