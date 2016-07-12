#ifndef MRB_LMDB_H
#define MRB_LMDB_H

#include <stdint.h>
#include "lmdb.h"
#include <string.h>
#include <mruby.h>
#include <mruby/data.h>
#include <mruby/class.h>
#include <mruby/string.h>
#include <mruby/array.h>
#include <mruby/dump.h>
#include <errno.h>
#include <mruby/error.h>
#include <mruby/hash.h>
#include <mruby/variable.h>

#define LMDB_STAT (mrb_class_get_under(mrb, mrb_module_get(mrb, "MDB"), "Stat"))
#define LMDB_ENV_INFO (mrb_class_get_under(mrb, mrb_class_get_under(mrb, mrb_module_get(mrb, "MDB"), "Env"), "Info"))

static void
mrb_mdb_env_free(mrb_state* mrb, void* p)
{
    mdb_env_close((MDB_env*)p);
}

static const struct mrb_data_type mdb_env_type = {
    "$mrb_i_mdb_env", mrb_mdb_env_free,
};

static void
mrb_mdb_txn_free(mrb_state* mrb, void* p)
{
    mdb_txn_abort((MDB_txn*)p);
}

static const struct mrb_data_type mdb_txn_type = {
    "$mrb_i_mdb_txn", mrb_mdb_txn_free,
};

static void
mrb_mdb_cursor_free(mrb_state* mrb, void* p)
{
    mdb_cursor_close((MDB_cursor*)p);
}

static const struct mrb_data_type mdb_cursor_type = {
    "$mrb_i_mdb_cursor", mrb_mdb_cursor_free,
};


#endif
