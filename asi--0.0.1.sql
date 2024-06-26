-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION asi" to load this file. \quit

CREATE FUNCTION asi_try_exclusive_lock(integer, integer)
RETURNS boolean
AS 'MODULE_PATHNAME', 'asi_try_exclusive_lock'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION asi_try_shared_lock(integer, integer)
RETURNS boolean
AS 'MODULE_PATHNAME', 'asi_try_shared_lock'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION asi_try_intent_lock(integer, integer)
RETURNS boolean
AS 'MODULE_PATHNAME', 'asi_try_intent_lock'
LANGUAGE C VOLATILE STRICT;

-- CREATE FUNCTION asi_try_table_exclusive_lock(integer, integer)
-- RETURNS boolean
-- AS 'MODULE_PATHNAME', 'asi_try_table_exclusive_lock'
-- LANGUAGE C VOLATILE STRICT;

-- CREATE FUNCTION asi_try_table_shared_lock(integer, integer)
-- RETURNS boolean
-- AS 'MODULE_PATHNAME', 'asi_try_table_shared_lock'
-- LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION asi_validate_intent_lock(integer, integer)
RETURNS void 
AS 'MODULE_PATHNAME', 'asi_validate_intent_lock'
LANGUAGE C VOLATILE STRICT;

-- CREATE FUNCTION asi_release_intent_lock(integer, integer)
-- RETURNS void
-- AS 'MODULE_PATHNAME', 'asi_release_intent_lock'
-- LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION asi_release_exclusive_lock(integer, integer)
RETURNS void
AS 'MODULE_PATHNAME', 'asi_release_exclusive_lock'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION asi_release_shared_lock(integer, integer)
RETURNS void
AS 'MODULE_PATHNAME', 'asi_release_shared_lock'
LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION asi_intent_lock(integer, integer)
RETURNS boolean
AS 'MODULE_PATHNAME', 'asi_intent_lock'
LANGUAGE C VOLATILE STRICT;