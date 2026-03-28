#ifndef MRB_LMDB_INTERNAL_H
#define MRB_LMDB_INTERNAL_H

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdbool.h>

#include "lmdb.h"

#include <mruby.h>
#include <mruby/data.h>
#include <mruby/class.h>
#include <mruby/string.h>
#include <mruby/array.h>
#include <mruby/hash.h>
#include <mruby/variable.h>
#include <mruby/error.h>
#include <mruby/presym.h>
#include <mruby/branch_pred.h>
#include <mruby/num_helpers.h>

/* ── Data type descriptors ────────────────────────────────────────────────── */

static void mrb_mdb_env_free(mrb_state *mrb, void *p) {
  if (p) mdb_env_close((MDB_env *)p);
}

static void mrb_mdb_txn_free(mrb_state *mrb, void *p) {
  if (p) mdb_txn_abort((MDB_txn *)p);
}

static void mrb_mdb_cursor_free(mrb_state *mrb, void *p) {
  if (p) mdb_cursor_close((MDB_cursor *)p);
}

static const struct mrb_data_type mdb_env_type = {
  "MDB::Env", mrb_mdb_env_free,
};

static const struct mrb_data_type mdb_txn_type = {
  "MDB::Txn", mrb_mdb_txn_free,
};

static const struct mrb_data_type mdb_cursor_type = {
  "MDB::Cursor", mrb_mdb_cursor_free,
};

/* IOError for closed handles */
#ifndef E_IO_ERROR
#define E_IO_ERROR (mrb_exc_get(mrb, "IOError"))
#endif

/* ── Error handling ───────────────────────────────────────────────────────── */

mrb_noreturn static void
mrb_mdb_raise(mrb_state *mrb, int rc, const char *func)
{
  if (rc > 0) {
    mrb_sys_fail(mrb, func);
  }
  struct RClass *mdb_error = mrb_class_get_under_id(mrb,
    mrb_module_get_id(mrb, MRB_SYM(MDB)), MRB_SYM(Error));
  mrb_value error2class = mrb_const_get(mrb,
    mrb_obj_value(mdb_error), MRB_SYM(Error2Class));
  mrb_value key = mrb_fixnum_value(rc);
  mrb_value cls = mrb_hash_fetch(mrb, error2class, key, mrb_obj_value(mdb_error));

  mrb_raisef(mrb, (struct RClass *)mrb_ptr(cls), "%s: %s", func, mdb_strerror(rc));
}

/* ── Safe data pointer extraction ─────────────────────────────────────────── */

static MDB_env *
mrb_mdb_env_get(mrb_state *mrb, mrb_value self)
{
  MDB_env *p = (MDB_env *)mrb_data_check_get_ptr(mrb, self, &mdb_env_type);
  if (likely(p))
    return p;
  mrb_raise(mrb, E_IO_ERROR, "closed MDB::Env");
}

static MDB_txn *
mrb_mdb_txn_get(mrb_state *mrb, mrb_value self)
{
  MDB_txn *p = (MDB_txn *)mrb_data_check_get_ptr(mrb, self, &mdb_txn_type);
  if (likely(p))
    return p;
  mrb_raise(mrb, E_RUNTIME_ERROR, "closed MDB::Txn");
}

static MDB_cursor *
mrb_mdb_cursor_get(mrb_state *mrb, mrb_value self)
{
  MDB_cursor *p = (MDB_cursor *)mrb_data_check_get_ptr(mrb, self, &mdb_cursor_type);
  if (likely(p))
    return p;
  mrb_raise(mrb, E_RUNTIME_ERROR, "closed MDB::Cursor");
}

static MDB_env *
mrb_mdb_database_env(mrb_state *mrb, mrb_value self)
{
  mrb_value env_v = mrb_iv_get(mrb, self, MRB_IVSYM(env));
  return mrb_mdb_env_get(mrb, env_v);
}

static MDB_dbi
mrb_mdb_database_dbi(mrb_state *mrb, mrb_value self)
{
  mrb_value dbi_v = mrb_iv_get(mrb, self, MRB_IVSYM(dbi));
  mrb_int dbi = mrb_integer(dbi_v);
  if (likely(dbi >= 0 && (uint64_t)dbi <= UINT_MAX))
    return (MDB_dbi)dbi;
  mrb_raise(mrb, E_RANGE_ERROR, "dbi out of range");
}

/* ── Range validation helpers ─────────────────────────────────────────────── */

static unsigned int
mrb_mdb_flags(mrb_state *mrb, mrb_int flags)
{
  if (likely(flags >= 0 && (uint64_t)flags <= UINT_MAX))
    return (unsigned int)flags;
  mrb_raise(mrb, E_RANGE_ERROR, "flags out of range");
}

static MDB_dbi
mrb_mdb_dbi(mrb_state *mrb, mrb_int dbi)
{
  if (likely(dbi >= 0 && (uint64_t)dbi <= UINT_MAX))
    return (MDB_dbi)dbi;
  mrb_raise(mrb, E_RANGE_ERROR, "dbi out of range");
}

static MDB_cursor_op
mrb_mdb_cursor_op(mrb_state *mrb, mrb_int op)
{
  if (likely(op >= MDB_FIRST && op <= MDB_PREV_MULTIPLE))
    return (MDB_cursor_op)op;
  mrb_raisef(mrb, E_RANGE_ERROR, "cursor op %i out of range", (mrb_int)op);
}

/* ── MDB_val helpers ──────────────────────────────────────────────────────── */

static mrb_value
mrb_mdb_val_to_str(mrb_state *mrb, const MDB_val *val)
{
  return mrb_str_new(mrb, (const char *)val->mv_data, (mrb_int)val->mv_size);
}

/* ── Stat helper ──────────────────────────────────────────────────────────── */

static mrb_value
mrb_mdb_stat_to_value(mrb_state *mrb, const MDB_stat *stat)
{
  mrb_value args[6] = {
    mrb_convert_uint(mrb, stat->ms_psize),
    mrb_convert_uint(mrb, stat->ms_depth),
    mrb_convert_size_t(mrb, stat->ms_branch_pages),
    mrb_convert_size_t(mrb, stat->ms_leaf_pages),
    mrb_convert_size_t(mrb, stat->ms_overflow_pages),
    mrb_convert_size_t(mrb, stat->ms_entries),
  };
  return mrb_obj_new(mrb, mrb_class_get_under_id(mrb, mrb_module_get_id(mrb, MRB_SYM(MDB)), MRB_SYM(Stat)), 6, args);
}

#endif /* MRB_LMDB_INTERNAL_H */