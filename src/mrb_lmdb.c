#include "mruby/lmdb.h"
#include "mrb_lmdb.h"

/* ========================================================================
 * Integer ↔ binary helpers (for MDB_INTEGERKEY)
 * ======================================================================== */

static mrb_value
mrb_fix2bin(mrb_state *mrb, mrb_value self)
{
  mrb_int number = mrb_integer(self);

#if MRB_INT_BIT == 64
  const size_t len = 8;
  uint64_t u = (uint64_t)number;
#elif MRB_INT_BIT == 32
  const size_t len = 4;
  uint32_t u = (uint32_t)number;
#elif MRB_INT_BIT == 16
  const size_t len = 2;
  uint16_t u = (uint16_t)number;
#else
# error "Unsupported MRB_INT_BIT"
#endif

  mrb_value str = mrb_str_new(mrb, NULL, len);
  uint8_t *p = (uint8_t *)RSTRING_PTR(str);

#if MRB_INT_BIT == 64
  p[0] = (uint8_t)(u >> 56);
  p[1] = (uint8_t)(u >> 48);
  p[2] = (uint8_t)(u >> 40);
  p[3] = (uint8_t)(u >> 32);
  p[4] = (uint8_t)(u >> 24);
  p[5] = (uint8_t)(u >> 16);
  p[6] = (uint8_t)(u >>  8);
  p[7] = (uint8_t)(u);
#elif MRB_INT_BIT == 32
  p[0] = (uint8_t)(u >> 24);
  p[1] = (uint8_t)(u >> 16);
  p[2] = (uint8_t)(u >>  8);
  p[3] = (uint8_t)(u);
#elif MRB_INT_BIT == 16
  p[0] = (uint8_t)(u >> 8);
  p[1] = (uint8_t)(u);
#endif

  return str;
}

static mrb_value
mrb_bin2fix(mrb_state *mrb, mrb_value self)
{
  const size_t len = RSTRING_LEN(self);
  const uint8_t *p = (const uint8_t *)RSTRING_PTR(self);

#if MRB_INT_BIT == 64
  if (len != 8) mrb_raise(mrb, E_TYPE_ERROR, "binary string size mismatch for Integer");
  uint64_t u =
      ((uint64_t)p[0] << 56) |
      ((uint64_t)p[1] << 48) |
      ((uint64_t)p[2] << 40) |
      ((uint64_t)p[3] << 32) |
      ((uint64_t)p[4] << 24) |
      ((uint64_t)p[5] << 16) |
      ((uint64_t)p[6] <<  8) |
      ((uint64_t)p[7]);
#elif MRB_INT_BIT == 32
  if (len != 4) mrb_raise(mrb, E_TYPE_ERROR, "binary string size mismatch for Integer");
  uint32_t u =
      ((uint32_t)p[0] << 24) |
      ((uint32_t)p[1] << 16) |
      ((uint32_t)p[2] <<  8) |
      ((uint32_t)p[3]);
#elif MRB_INT_BIT == 16
  if (len != 2) mrb_raise(mrb, E_TYPE_ERROR, "binary string size mismatch for Integer");
  uint16_t u =
      ((uint16_t)p[0] << 8) |
      ((uint16_t)p[1]);
#endif

  return mrb_int_value(mrb, (mrb_int)u);
}


/* ========================================================================
 * MDB::Env
 * ======================================================================== */

static mrb_value
mrb_mdb_env_init(mrb_state *mrb, mrb_value self)
{
  MDB_env *env;
  int rc = mdb_env_create(&env);
  mrb_mdb_raise(mrb, rc, "mdb_env_create");
  mrb_data_init(self, env, &mdb_env_type);
  return self;
}

static mrb_value
mrb_mdb_env_open(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  const char *path;
  mrb_int flags = 0, mode = 0600;
  mrb_get_args(mrb, "z|ii", &path, &flags, &mode);

  int rc = mdb_env_open(env, path,
    mrb_mdb_flags(mrb, flags), (mdb_mode_t)mode);
  mrb_mdb_raise(mrb, rc, "mdb_env_open");
  return self;
}

static mrb_value
mrb_mdb_env_copy_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  const char *path;
  mrb_int flags = 0;
  mrb_get_args(mrb, "z|i", &path, &flags);

  int rc;
  if (flags != 0)
    rc = mdb_env_copy2(env, path, mrb_mdb_flags(mrb, flags));
  else
    rc = mdb_env_copy(env, path);
  mrb_mdb_raise(mrb, rc, "mdb_env_copy");
  return self;
}

static mrb_value
mrb_mdb_env_stat_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  MDB_stat stat;
  int rc = mdb_env_stat(env, &stat);
  mrb_mdb_raise(mrb, rc, "mdb_env_stat");
  return mrb_mdb_stat_to_value(mrb, &stat);
}

static mrb_value
mrb_mdb_env_info_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  MDB_envinfo info;
  int rc = mdb_env_info(env, &info);
  mrb_mdb_raise(mrb, rc, "mdb_env_info");

  mrb_value args[6] = {
    info.me_mapaddr ? mrb_cptr_value(mrb, info.me_mapaddr) : mrb_nil_value(),
    mrb_convert_size_t(mrb, info.me_mapsize),
    mrb_convert_size_t(mrb, info.me_last_pgno),
    mrb_convert_size_t(mrb, info.me_last_txnid),
    mrb_convert_uint(mrb, info.me_maxreaders),
    mrb_convert_uint(mrb, info.me_numreaders),
  };
  return mrb_obj_new(mrb, mrb_class_get_under_id(mrb, mrb_class_get_under_id(mrb, mrb_module_get_id(mrb, MRB_SYM(MDB)), MRB_SYM(Env)), MRB_SYM(Info)), 6, args);
}

static mrb_value
mrb_mdb_env_sync_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  mrb_bool force = FALSE;
  mrb_get_args(mrb, "|b", &force);
  int rc = mdb_env_sync(env, (int)force);
  mrb_mdb_raise(mrb, rc, "mdb_env_sync");
  return self;
}

static mrb_value
mrb_mdb_env_close_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = (MDB_env *)mrb_data_check_get_ptr(mrb, self, &mdb_env_type);
  if (env) {
    mdb_env_close(env);
    mrb_data_init(self, NULL, NULL);
    return mrb_true_value();
  }
  return mrb_false_value();
}

static mrb_value
mrb_mdb_env_set_flags_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  mrb_int flags = 0;
  mrb_bool onoff = TRUE;
  mrb_get_args(mrb, "|ib", &flags, &onoff);
  int rc = mdb_env_set_flags(env, mrb_mdb_flags(mrb, flags), (int)onoff);
  mrb_mdb_raise(mrb, rc, "mdb_env_set_flags");
  return self;
}

static mrb_value
mrb_mdb_env_get_flags_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  unsigned int flags;
  int rc = mdb_env_get_flags(env, &flags);
  mrb_mdb_raise(mrb, rc, "mdb_env_get_flags");
  return mrb_convert_uint(mrb, flags);
}

static mrb_value
mrb_mdb_env_get_path_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  const char *path;
  int rc = mdb_env_get_path(env, &path);
  mrb_mdb_raise(mrb, rc, "mdb_env_get_path");
  return mrb_str_new_cstr(mrb, path);
}

static mrb_value
mrb_mdb_env_set_mapsize_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  mrb_int size;
  mrb_get_args(mrb, "i", &size);
  if (unlikely(size < 0))
    mrb_raise(mrb, E_RANGE_ERROR, "mapsize must be non-negative");
  int rc = mdb_env_set_mapsize(env, (size_t)size);
  mrb_mdb_raise(mrb, rc, "mdb_env_set_mapsize");
  return self;
}

static mrb_value
mrb_mdb_env_set_maxreaders_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  mrb_int readers;
  mrb_get_args(mrb, "i", &readers);
  if (unlikely(readers < 0 || (uint64_t)readers > UINT_MAX))
    mrb_raise(mrb, E_RANGE_ERROR, "maxreaders out of range");
  int rc = mdb_env_set_maxreaders(env, (unsigned int)readers);
  mrb_mdb_raise(mrb, rc, "mdb_env_set_maxreaders");
  return self;
}

static mrb_value
mrb_mdb_env_get_maxreaders_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  unsigned int readers;
  int rc = mdb_env_get_maxreaders(env, &readers);
  mrb_mdb_raise(mrb, rc, "mdb_env_get_maxreaders");
  return mrb_convert_uint(mrb, readers);
}

static mrb_value
mrb_mdb_env_set_maxdbs_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  mrb_int dbs;
  mrb_get_args(mrb, "i", &dbs);
  if (unlikely(dbs < 0 || (uint64_t)dbs > UINT_MAX))
    mrb_raise(mrb, E_RANGE_ERROR, "maxdbs out of range");
  int rc = mdb_env_set_maxdbs(env, (MDB_dbi)dbs);
  mrb_mdb_raise(mrb, rc, "mdb_env_set_maxdbs");
  return self;
}

static mrb_value
mrb_mdb_env_get_maxkeysize_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  return mrb_convert_int(mrb, mdb_env_get_maxkeysize(env));
}

static mrb_value
mrb_mdb_reader_check_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  int dead = 0;
  int rc = mdb_reader_check(env, &dead);
  mrb_mdb_raise(mrb, rc, "mdb_reader_check");
  return mrb_convert_int(mrb, dead);
}

/* ========================================================================
 * MDB::Txn
 * ======================================================================== */

static mrb_value
mrb_mdb_txn_init(mrb_state *mrb, mrb_value self)
{
  mrb_value env_v;
  mrb_int flags = 0;
  mrb_value parent_v = mrb_nil_value();
  mrb_get_args(mrb, "o|io", &env_v, &flags, &parent_v);

  MDB_env *env = mrb_mdb_env_get(mrb, env_v);
  MDB_txn *parent = NULL;
  if (!mrb_nil_p(parent_v))
    parent = mrb_mdb_txn_get(mrb, parent_v);

  MDB_txn *txn;
  int rc = mdb_txn_begin(env, parent, mrb_mdb_flags(mrb, flags), &txn);
  mrb_mdb_raise(mrb, rc, "mdb_txn_begin");
  mrb_data_init(self, txn, &mdb_txn_type);
  return self;
}

static mrb_value
mrb_mdb_txn_commit_m(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn = mrb_mdb_txn_get(mrb, self);
  int rc = mdb_txn_commit(txn);
  mrb_data_init(self, NULL, NULL);  /* always detach — commit consumes txn */
  mrb_mdb_raise(mrb, rc, "mdb_txn_commit");
  return mrb_true_value();
}

static mrb_value
mrb_mdb_txn_abort_m(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn = (MDB_txn *)mrb_data_check_get_ptr(mrb, self, &mdb_txn_type);
  if (txn) {
    mdb_txn_abort(txn);
    mrb_data_init(self, NULL, NULL);
    return mrb_true_value();
  }
  return mrb_false_value();
}

static mrb_value
mrb_mdb_txn_reset_m(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn = mrb_mdb_txn_get(mrb, self);
  mdb_txn_reset(txn);
  return self;
}

static mrb_value
mrb_mdb_txn_renew_m(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn = mrb_mdb_txn_get(mrb, self);
  int rc = mdb_txn_renew(txn);
  mrb_mdb_raise(mrb, rc, "mdb_txn_renew");
  return self;
}

/* ========================================================================
 * MDB::Dbi (module functions)
 * ======================================================================== */

static mrb_value
mrb_mdb_dbi_open_m(mrb_state *mrb, mrb_value self)
{
  mrb_value txn_v;
  mrb_int flags = 0;
  const char *name = NULL;
  mrb_get_args(mrb, "o|iz!", &txn_v, &flags, &name);

  MDB_txn *txn = mrb_mdb_txn_get(mrb, txn_v);
  MDB_dbi dbi;
  int rc = mdb_dbi_open(txn, name, mrb_mdb_flags(mrb, flags), &dbi);
  mrb_mdb_raise(mrb, rc, "mdb_dbi_open");
  return mrb_convert_uint(mrb, dbi);
}

static mrb_value
mrb_mdb_dbi_flags_m(mrb_state *mrb, mrb_value self)
{
  mrb_value txn_v;
  mrb_int dbi;
  mrb_get_args(mrb, "oi", &txn_v, &dbi);

  MDB_txn *txn = mrb_mdb_txn_get(mrb, txn_v);
  unsigned int flags;
  int rc = mdb_dbi_flags(txn, mrb_mdb_dbi(mrb, dbi), &flags);
  mrb_mdb_raise(mrb, rc, "mdb_dbi_flags");
  return mrb_convert_uint(mrb, flags);
}

/* ========================================================================
 * MDB module functions: get, put, del, drop, stat
 * ======================================================================== */

static mrb_value
mrb_mdb_get_m(mrb_state *mrb, mrb_value self)
{
  mrb_value txn_v, key_obj;
  mrb_int dbi;
  mrb_get_args(mrb, "oio", &txn_v, &dbi, &key_obj);

  MDB_txn *txn = mrb_mdb_txn_get(mrb, txn_v);
  key_obj = mrb_str_to_str(mrb, key_obj);

  MDB_val key = { (size_t)RSTRING_LEN(key_obj), RSTRING_PTR(key_obj) };
  MDB_val data;

  int rc = mdb_get(txn, mrb_mdb_dbi(mrb, dbi), &key, &data);
  if (rc == MDB_NOTFOUND) return mrb_nil_value();
  mrb_mdb_raise(mrb, rc, "mdb_get");
  return mrb_mdb_val_to_str(mrb, &data);
}

static mrb_value
mrb_mdb_put_m(mrb_state *mrb, mrb_value self)
{
  mrb_value txn_v, key_obj, data_obj;
  mrb_int dbi, flags = 0;
  mrb_get_args(mrb, "oioo|i", &txn_v, &dbi, &key_obj, &data_obj, &flags);

  MDB_txn *txn = mrb_mdb_txn_get(mrb, txn_v);

  key_obj = mrb_str_to_str(mrb, key_obj);
  data_obj = mrb_str_to_str(mrb, data_obj);

  MDB_val key  = { (size_t)RSTRING_LEN(key_obj),  RSTRING_PTR(key_obj) };
  MDB_val data = { (size_t)RSTRING_LEN(data_obj), RSTRING_PTR(data_obj) };

  int rc = mdb_put(txn, mrb_mdb_dbi(mrb, dbi), &key, &data,
                   mrb_mdb_flags(mrb, flags));
  mrb_mdb_raise(mrb, rc, "mdb_put");
  return self;
}

static mrb_value
mrb_mdb_del_m(mrb_state *mrb, mrb_value self)
{
  mrb_value txn_v, key_obj, data_obj = mrb_nil_value();
  mrb_int dbi;
  mrb_get_args(mrb, "oio|o", &txn_v, &dbi, &key_obj, &data_obj);

  MDB_txn *txn = mrb_mdb_txn_get(mrb, txn_v);

  key_obj = mrb_str_to_str(mrb, key_obj);

  MDB_val key = { (size_t)RSTRING_LEN(key_obj), RSTRING_PTR(key_obj) };
  MDB_val data_v;
  MDB_val *data_ptr = NULL;

  if (!mrb_nil_p(data_obj)) {
    data_obj = mrb_str_to_str(mrb, data_obj);

    data_v.mv_size = (size_t)RSTRING_LEN(data_obj);
    data_v.mv_data = RSTRING_PTR(data_obj);
    data_ptr = &data_v;
  }

  int rc = mdb_del(txn, mrb_mdb_dbi(mrb, dbi), &key, data_ptr);
  if (rc == MDB_NOTFOUND) return mrb_nil_value();
  mrb_mdb_raise(mrb, rc, "mdb_del");
  return mrb_true_value();
}

static mrb_value
mrb_mdb_drop_m(mrb_state *mrb, mrb_value self)
{
  mrb_value txn_v;
  mrb_int dbi;
  mrb_bool del = FALSE;
  mrb_get_args(mrb, "oi|b", &txn_v, &dbi, &del);

  MDB_txn *txn = mrb_mdb_txn_get(mrb, txn_v);
  int rc = mdb_drop(txn, mrb_mdb_dbi(mrb, dbi), (int)del);
  mrb_mdb_raise(mrb, rc, "mdb_drop");
  return self;
}

static mrb_value
mrb_mdb_stat_m(mrb_state *mrb, mrb_value self)
{
  mrb_value txn_v;
  mrb_int dbi;
  mrb_get_args(mrb, "oi", &txn_v, &dbi);

  MDB_txn *txn = mrb_mdb_txn_get(mrb, txn_v);
  MDB_stat stat;
  int rc = mdb_stat(txn, mrb_mdb_dbi(mrb, dbi), &stat);
  mrb_mdb_raise(mrb, rc, "mdb_stat");
  return mrb_mdb_stat_to_value(mrb, &stat);
}

/* ========================================================================
 * MDB::Cursor
 * ======================================================================== */

static mrb_value
mrb_mdb_cursor_init(mrb_state *mrb, mrb_value self)
{
  mrb_value txn_v;
  mrb_int dbi;
  mrb_get_args(mrb, "oi", &txn_v, &dbi);

  MDB_txn *txn = mrb_mdb_txn_get(mrb, txn_v);
  MDB_cursor *cursor;
  int rc = mdb_cursor_open(txn, mrb_mdb_dbi(mrb, dbi), &cursor);
  mrb_mdb_raise(mrb, rc, "mdb_cursor_open");
  mrb_data_init(self, cursor, &mdb_cursor_type);
  return self;
}

static mrb_value
mrb_mdb_cursor_close_m(mrb_state *mrb, mrb_value self)
{
  MDB_cursor *cursor = (MDB_cursor *)mrb_data_check_get_ptr(mrb, self, &mdb_cursor_type);
  if (cursor) {
    mdb_cursor_close(cursor);
    mrb_data_init(self, NULL, NULL);
    return mrb_true_value();
  }
  return mrb_false_value();
}

static mrb_value
mrb_mdb_cursor_renew_m(mrb_state *mrb, mrb_value self)
{
  MDB_cursor *cursor = mrb_mdb_cursor_get(mrb, self);
  mrb_value txn_v;
  mrb_get_args(mrb, "o", &txn_v);
  MDB_txn *txn = mrb_mdb_txn_get(mrb, txn_v);
  int rc = mdb_cursor_renew(txn, cursor);
  mrb_mdb_raise(mrb, rc, "mdb_cursor_renew");
  return self;
}

static mrb_value
mrb_mdb_cursor_get_m(mrb_state *mrb, mrb_value self)
{
  MDB_cursor *cursor = mrb_mdb_cursor_get(mrb, self);
  mrb_int cursor_op;
  mrb_value key_obj = mrb_nil_value(), data_obj = mrb_nil_value();
  mrb_get_args(mrb, "i|oo", &cursor_op, &key_obj, &data_obj);

  MDB_val key = { 0, NULL };
  MDB_val data = { 0, NULL };

  if (!mrb_nil_p(key_obj)) {
    key_obj = mrb_str_to_str(mrb, key_obj);

    key.mv_size = (size_t)RSTRING_LEN(key_obj);
    key.mv_data = RSTRING_PTR(key_obj);
  }
  if (!mrb_nil_p(data_obj)) {
    data_obj = mrb_str_to_str(mrb, data_obj);

    data.mv_size = (size_t)RSTRING_LEN(data_obj);
    data.mv_data = RSTRING_PTR(data_obj);
  }

  int rc = mdb_cursor_get(cursor, &key, &data, (MDB_cursor_op)cursor_op);
  if (rc == MDB_NOTFOUND) return mrb_nil_value();
  mrb_mdb_raise(mrb, rc, "mdb_cursor_get");

  mrb_value k = mrb_mdb_val_to_str(mrb, &key);
  mrb_value v = mrb_mdb_val_to_str(mrb, &data);
  return mrb_assoc_new(mrb, k, v);
}

static mrb_value
mrb_mdb_cursor_put_m(mrb_state *mrb, mrb_value self)
{
  MDB_cursor *cursor = mrb_mdb_cursor_get(mrb, self);
  mrb_value key_obj, data_obj;
  mrb_int flags = 0;
  mrb_get_args(mrb, "oo|i", &key_obj, &data_obj, &flags);

  key_obj = mrb_str_to_str(mrb, key_obj);
  data_obj = mrb_str_to_str(mrb, data_obj);

  MDB_val key  = { (size_t)RSTRING_LEN(key_obj),  RSTRING_PTR(key_obj) };
  MDB_val data = { (size_t)RSTRING_LEN(data_obj), RSTRING_PTR(data_obj) };

  int rc = mdb_cursor_put(cursor, &key, &data, mrb_mdb_flags(mrb, flags));
  mrb_mdb_raise(mrb, rc, "mdb_cursor_put");
  return self;
}

static mrb_value
mrb_mdb_cursor_del_m(mrb_state *mrb, mrb_value self)
{
  MDB_cursor *cursor = mrb_mdb_cursor_get(mrb, self);
  mrb_int flags = 0;
  mrb_get_args(mrb, "|i", &flags);
  int rc = mdb_cursor_del(cursor, mrb_mdb_flags(mrb, flags));
  mrb_mdb_raise(mrb, rc, "mdb_cursor_del");
  return self;
}

static mrb_value
mrb_mdb_cursor_count_m(mrb_state *mrb, mrb_value self)
{
  MDB_cursor *cursor = mrb_mdb_cursor_get(mrb, self);
  size_t count;
  int rc = mdb_cursor_count(cursor, &count);
  mrb_mdb_raise(mrb, rc, "mdb_cursor_count");
  return mrb_convert_size_t(mrb, count);
}

/* ========================================================================
 * Public C API
 * ======================================================================== */

MRB_API mrb_value
mrb_lmdb_get(mrb_state *mrb, MDB_txn *txn, MDB_dbi dbi,
             const void *key_data, size_t key_len)
{
  MDB_val key = { key_len, (void *)key_data };
  MDB_val data;
  int rc = mdb_get(txn, dbi, &key, &data);
  if (rc == MDB_NOTFOUND) return mrb_nil_value();
  mrb_mdb_raise(mrb, rc, "mdb_get");
  return mrb_str_new(mrb, (const char *)data.mv_data, (mrb_int)data.mv_size);
}

MRB_API void
mrb_lmdb_put(mrb_state *mrb, MDB_txn *txn, MDB_dbi dbi,
             const void *key_data, size_t key_len,
             const void *val_data, size_t val_len,
             unsigned int flags)
{
  MDB_val key = { key_len, (void *)key_data };
  MDB_val data = { val_len, (void *)val_data };
  int rc = mdb_put(txn, dbi, &key, &data, flags);
  mrb_mdb_raise(mrb, rc, "mdb_put");
}

MRB_API mrb_bool
mrb_lmdb_del(mrb_state *mrb, MDB_txn *txn, MDB_dbi dbi,
             const void *key_data, size_t key_len)
{
  MDB_val key = { key_len, (void *)key_data };
  int rc = mdb_del(txn, dbi, &key, NULL);
  if (rc == MDB_NOTFOUND) return FALSE;
  mrb_mdb_raise(mrb, rc, "mdb_del");
  return TRUE;
}

/* ========================================================================
 * Gem init / final
 * ======================================================================== */

MRB_API void
mrb_mruby_lmdb_gem_init(mrb_state *mrb)
{
  struct RClass *mdb_mod;
  struct RClass *mdb_error_class;
  struct RClass *mdb_env_class;
  struct RClass *mdb_txn_class;
  struct RClass *mdb_cursor_class;
  struct RClass *mdb_dbi_mod;
  /* ── Module & classes ─────────────────────────────────────────────────── */
  mdb_mod = mrb_define_module_id(mrb, MRB_SYM(MDB));

  mrb_define_const(mrb, mdb_mod, "VERSION",
    mrb_str_new_lit(mrb, MDB_VERSION_STRING));

  /* Flags */
  #define DEFINE_MDB_CONST(name) \
    mrb_define_const(mrb, mdb_mod, #name, mrb_int_value(mrb, MDB_##name))

  DEFINE_MDB_CONST(FIXEDMAP);
  DEFINE_MDB_CONST(NOSUBDIR);
  DEFINE_MDB_CONST(NOSYNC);
  DEFINE_MDB_CONST(RDONLY);
  DEFINE_MDB_CONST(NOMETASYNC);
  DEFINE_MDB_CONST(WRITEMAP);
  DEFINE_MDB_CONST(MAPASYNC);
  DEFINE_MDB_CONST(NOTLS);
  DEFINE_MDB_CONST(NOLOCK);
  DEFINE_MDB_CONST(NORDAHEAD);
  DEFINE_MDB_CONST(NOMEMINIT);
  DEFINE_MDB_CONST(REVERSEKEY);
  DEFINE_MDB_CONST(DUPSORT);
  DEFINE_MDB_CONST(INTEGERKEY);
  DEFINE_MDB_CONST(DUPFIXED);
  DEFINE_MDB_CONST(INTEGERDUP);
  DEFINE_MDB_CONST(REVERSEDUP);
  DEFINE_MDB_CONST(CREATE);
  DEFINE_MDB_CONST(NOOVERWRITE);
  DEFINE_MDB_CONST(NODUPDATA);
  DEFINE_MDB_CONST(CURRENT);
  DEFINE_MDB_CONST(RESERVE);
  DEFINE_MDB_CONST(APPEND);
  DEFINE_MDB_CONST(APPENDDUP);
  DEFINE_MDB_CONST(MULTIPLE);
  DEFINE_MDB_CONST(CP_COMPACT);

  #undef DEFINE_MDB_CONST

  /* ── Error class + subclasses ────────────────────────────────────────── */
  mdb_error_class = mrb_define_class_under_id(mrb, mdb_mod,
    MRB_SYM(Error), E_RUNTIME_ERROR);

  mrb_value error2class = mrb_hash_new(mrb);
  mrb_define_const_id(mrb, mdb_error_class, MRB_SYM(Error2Class), error2class);

  #define mrb_lmdb_define_error(MDB_ERROR, RB_CLASS_NAME) \
    do { \
      int ai = mrb_gc_arena_save(mrb); \
      struct RClass *err = mrb_define_class_under(mrb, mdb_mod, \
        RB_CLASS_NAME, mdb_error_class); \
      mrb_hash_set(mrb, error2class, mrb_int_value(mrb, MDB_ERROR), \
        mrb_obj_value(err)); \
      mrb_gc_arena_restore(mrb, ai); \
    } while(0)

  #include "known_errors_def.cstub"
  #undef mrb_lmdb_define_error

  /* ── MDB::Env ────────────────────────────────────────────────────────── */
  mdb_env_class = mrb_define_class_under_id(mrb, mdb_mod,
    MRB_SYM(Env), mrb->object_class);
  MRB_SET_INSTANCE_TT(mdb_env_class, MRB_TT_CDATA);

  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM(initialize),   mrb_mdb_env_init,           MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM(open),         mrb_mdb_env_open,           MRB_ARGS_ARG(1, 2));
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM(copy),         mrb_mdb_env_copy_m,         MRB_ARGS_ARG(1, 1));
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM(stat),         mrb_mdb_env_stat_m,         MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM(info),         mrb_mdb_env_info_m,         MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM(sync),         mrb_mdb_env_sync_m,         MRB_ARGS_OPT(1));
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM(close),        mrb_mdb_env_close_m,        MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM(set_flags),    mrb_mdb_env_set_flags_m,    MRB_ARGS_OPT(2));
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM(flags),        mrb_mdb_env_get_flags_m,    MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM(path),         mrb_mdb_env_get_path_m,     MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM_E(mapsize),    mrb_mdb_env_set_mapsize_m,  MRB_ARGS_REQ(1));
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM_E(maxreaders), mrb_mdb_env_set_maxreaders_m, MRB_ARGS_REQ(1));
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM(maxreaders),   mrb_mdb_env_get_maxreaders_m, MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM_E(maxdbs),     mrb_mdb_env_set_maxdbs_m,   MRB_ARGS_REQ(1));
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM(maxkeysize),   mrb_mdb_env_get_maxkeysize_m, MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class, MRB_SYM(reader_check), mrb_mdb_reader_check_m,     MRB_ARGS_NONE());


  /* ── MDB::Txn ────────────────────────────────────────────────────────── */
  mdb_txn_class = mrb_define_class_under_id(mrb, mdb_mod,
    MRB_SYM(Txn), mrb->object_class);
  MRB_SET_INSTANCE_TT(mdb_txn_class, MRB_TT_CDATA);

  mrb_define_method_id(mrb, mdb_txn_class, MRB_SYM(initialize), mrb_mdb_txn_init,     MRB_ARGS_ARG(1, 2));
  mrb_define_method_id(mrb, mdb_txn_class, MRB_SYM(commit),     mrb_mdb_txn_commit_m, MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_txn_class, MRB_SYM(abort),      mrb_mdb_txn_abort_m,  MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_txn_class, MRB_SYM(reset),      mrb_mdb_txn_reset_m,  MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_txn_class, MRB_SYM(renew),      mrb_mdb_txn_renew_m,  MRB_ARGS_NONE());

  /* ── MDB::Dbi ────────────────────────────────────────────────────────── */
  mdb_dbi_mod = mrb_define_module_under_id(mrb, mdb_mod, MRB_SYM(Dbi));
  mrb_define_module_function_id(mrb, mdb_dbi_mod, MRB_SYM(open),  mrb_mdb_dbi_open_m,  MRB_ARGS_ARG(1, 2));
  mrb_define_module_function_id(mrb, mdb_dbi_mod, MRB_SYM(flags), mrb_mdb_dbi_flags_m, MRB_ARGS_REQ(2));

  /* ── MDB module functions ────────────────────────────────────────────── */
  mrb_define_module_function_id(mrb, mdb_mod, MRB_SYM(stat), mrb_mdb_stat_m, MRB_ARGS_REQ(2));
  mrb_define_module_function_id(mrb, mdb_mod, MRB_SYM(get),  mrb_mdb_get_m,  MRB_ARGS_REQ(3));
  mrb_define_module_function_id(mrb, mdb_mod, MRB_SYM(put),  mrb_mdb_put_m,  MRB_ARGS_ARG(4, 1));
  mrb_define_module_function_id(mrb, mdb_mod, MRB_SYM(del),  mrb_mdb_del_m,  MRB_ARGS_ARG(3, 1));
  mrb_define_module_function_id(mrb, mdb_mod, MRB_SYM(drop), mrb_mdb_drop_m, MRB_ARGS_ARG(2, 1));

  /* ── MDB::Cursor ─────────────────────────────────────────────────────── */
  mdb_cursor_class = mrb_define_class_under_id(mrb, mdb_mod,
    MRB_SYM(Cursor), mrb->object_class);
  MRB_SET_INSTANCE_TT(mdb_cursor_class, MRB_TT_CDATA);

  #define DEFINE_CURSOR_CONST(name) \
    mrb_define_const(mrb, mdb_cursor_class, #name, mrb_int_value(mrb, MDB_##name))

  DEFINE_CURSOR_CONST(FIRST);     DEFINE_CURSOR_CONST(FIRST_DUP);
  DEFINE_CURSOR_CONST(GET_BOTH);  DEFINE_CURSOR_CONST(GET_BOTH_RANGE);
  DEFINE_CURSOR_CONST(GET_CURRENT); DEFINE_CURSOR_CONST(GET_MULTIPLE);
  DEFINE_CURSOR_CONST(LAST);      DEFINE_CURSOR_CONST(LAST_DUP);
  DEFINE_CURSOR_CONST(NEXT);      DEFINE_CURSOR_CONST(NEXT_DUP);
  DEFINE_CURSOR_CONST(NEXT_MULTIPLE); DEFINE_CURSOR_CONST(NEXT_NODUP);
  DEFINE_CURSOR_CONST(PREV);      DEFINE_CURSOR_CONST(PREV_DUP);
  DEFINE_CURSOR_CONST(PREV_NODUP);
  DEFINE_CURSOR_CONST(SET);       DEFINE_CURSOR_CONST(SET_KEY);
  DEFINE_CURSOR_CONST(SET_RANGE);
  DEFINE_CURSOR_CONST(PREV_MULTIPLE);

  #undef DEFINE_CURSOR_CONST

  mrb_define_method_id(mrb, mdb_cursor_class, MRB_SYM(initialize), mrb_mdb_cursor_init,    MRB_ARGS_REQ(2));
  mrb_define_method_id(mrb, mdb_cursor_class, MRB_SYM(close),      mrb_mdb_cursor_close_m, MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_cursor_class, MRB_SYM(renew),      mrb_mdb_cursor_renew_m, MRB_ARGS_REQ(1));
  mrb_define_method_id(mrb, mdb_cursor_class, MRB_SYM(get),        mrb_mdb_cursor_get_m,   MRB_ARGS_ARG(1, 2));
  mrb_define_method_id(mrb, mdb_cursor_class, MRB_SYM(put),        mrb_mdb_cursor_put_m,   MRB_ARGS_ARG(2, 1));
  mrb_define_method_id(mrb, mdb_cursor_class, MRB_SYM(del),        mrb_mdb_cursor_del_m,   MRB_ARGS_OPT(1));
  mrb_define_method_id(mrb, mdb_cursor_class, MRB_SYM(count),      mrb_mdb_cursor_count_m, MRB_ARGS_NONE());

  /* Cursor Ops hash for Ruby-side convenience methods */
  mrb_value cursor_ops = mrb_hash_new(mrb);
  mrb_define_const_id(mrb, mdb_cursor_class, MRB_SYM(Ops), cursor_ops);

  #define mrb_lmdb_define_cursor_op(MDB_CURSOR_OP, RB_CURSOR_OP_SYM) \
    do { \
      int ai = mrb_gc_arena_save(mrb); \
      mrb_hash_set(mrb, cursor_ops, \
        mrb_symbol_value(mrb_intern_lit(mrb, RB_CURSOR_OP_SYM)), \
        mrb_int_value(mrb, MDB_CURSOR_OP)); \
      mrb_gc_arena_restore(mrb, ai); \
    } while(0)

  #include "known_cursor_ops_def.cstub"
  #undef mrb_lmdb_define_cursor_op

  /* ── Integer#to_bin / String#to_fix — always big-endian ──────────────── */
  mrb_define_method_id(mrb, mrb->string_class,  MRB_SYM(to_fix), mrb_bin2fix, MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mrb->integer_class, MRB_SYM(to_bin), mrb_fix2bin, MRB_ARGS_NONE());
}

MRB_API void
mrb_mruby_lmdb_gem_final(mrb_state *mrb)
{
  (void)mrb;
}