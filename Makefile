# contrib/pgrowlocks/Makefile

EXTENSION = asi 
DATA = asi--0.0.1.sql
PGFILEDESC = "asi - Accelerated Snapshot Isolation"
MODULES = asi 
# OBJS = asi.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pgrowlocks
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
