#include "mruby/lmdb.h"
#include "mrb_lmdb.h"

/* ========================================================================
 * Integer <-> binary key helpers (native-endian, MDB_INTEGERKEY compatible)
 * ======================================================================== */

static mrb_value
mrb_lmdb_fix2bin(mrb_state *mrb, mrb_int number)
{
  mrb_value str = mrb_str_new(mrb, NULL, sizeof(mrb_int));
  uint8_t *p = (uint8_t *)RSTRING_PTR(str);

#ifdef MRB_ENDIAN_BIG
# if MRB_INT_BIT == 64
  p[0]=(uint8_t)(number>>56); p[1]=(uint8_t)(number>>48);
  p[2]=(uint8_t)(number>>40); p[3]=(uint8_t)(number>>32);
  p[4]=(uint8_t)(number>>24); p[5]=(uint8_t)(number>>16);
  p[6]=(uint8_t)(number>> 8); p[7]=(uint8_t)(number);
# elif MRB_INT_BIT == 32
  p[0]=(uint8_t)(number>>24); p[1]=(uint8_t)(number>>16);
  p[2]=(uint8_t)(number>> 8); p[3]=(uint8_t)(number);
# elif MRB_INT_BIT == 16
  p[0]=(uint8_t)(number>>8);  p[1]=(uint8_t)(number);
# else
  mrb_bug(mrb, "unknown MRB_INT_BIT");
# endif
#else /* little-endian */
# if MRB_INT_BIT == 64
  p[0]=(uint8_t)(number);     p[1]=(uint8_t)(number>> 8);
  p[2]=(uint8_t)(number>>16); p[3]=(uint8_t)(number>>24);
  p[4]=(uint8_t)(number>>32); p[5]=(uint8_t)(number>>40);
  p[6]=(uint8_t)(number>>48); p[7]=(uint8_t)(number>>56);
# elif MRB_INT_BIT == 32
  p[0]=(uint8_t)(number);     p[1]=(uint8_t)(number>> 8);
  p[2]=(uint8_t)(number>>16); p[3]=(uint8_t)(number>>24);
# elif MRB_INT_BIT == 16
  p[0]=(uint8_t)(number);     p[1]=(uint8_t)(number>>8);
# else
  mrb_bug(mrb, "unknown MRB_INT_BIT");
# endif
#endif

  return str;
}

static mrb_int
mrb_lmdb_bin2fix(mrb_state *mrb, const char *ptr, mrb_int len)
{
  if (len != sizeof(mrb_int))
    mrb_raise(mrb, E_TYPE_ERROR, "String is not encoded with Integer.to_bin");

  const uint8_t *p = (const uint8_t *)ptr;

#ifdef MRB_ENDIAN_BIG
# if MRB_INT_BIT == 64
  uint64_t u =
    ((uint64_t)p[0]<<56)|((uint64_t)p[1]<<48)|
    ((uint64_t)p[2]<<40)|((uint64_t)p[3]<<32)|
    ((uint64_t)p[4]<<24)|((uint64_t)p[5]<<16)|
    ((uint64_t)p[6]<< 8)|((uint64_t)p[7]);
# elif MRB_INT_BIT == 32
  uint32_t u =
    ((uint32_t)p[0]<<24)|((uint32_t)p[1]<<16)|
    ((uint32_t)p[2]<< 8)|((uint32_t)p[3]);
# elif MRB_INT_BIT == 16
  uint16_t u = ((uint16_t)p[0]<<8)|((uint16_t)p[1]);
# else
  mrb_bug(mrb, "unknown MRB_INT_BIT");
# endif
#else
# if MRB_INT_BIT == 64
  uint64_t u =
    ((uint64_t)p[0])    |((uint64_t)p[1]<< 8)|
    ((uint64_t)p[2]<<16)|((uint64_t)p[3]<<24)|
    ((uint64_t)p[4]<<32)|((uint64_t)p[5]<<40)|
    ((uint64_t)p[6]<<48)|((uint64_t)p[7]<<56);
# elif MRB_INT_BIT == 32
  uint32_t u =
    ((uint32_t)p[0])    |((uint32_t)p[1]<< 8)|
    ((uint32_t)p[2]<<16)|((uint32_t)p[3]<<24);
# elif MRB_INT_BIT == 16
  uint16_t u = ((uint16_t)p[0])|((uint16_t)p[1]<<8);
# else
  mrb_bug(mrb, "unknown MRB_INT_BIT");
# endif
#endif

  return (mrb_int)u;
}

static mrb_value
mrb_fix2bin_m(mrb_state *mrb, mrb_value self)
{
  return mrb_lmdb_fix2bin(mrb, mrb_integer(self));
}

static mrb_value
mrb_bin2fix_m(mrb_state *mrb, mrb_value self)
{
  return mrb_int_value(mrb,
    mrb_lmdb_bin2fix(mrb, RSTRING_PTR(self), RSTRING_LEN(self)));
}

/* ========================================================================
 * mrb_protect_error callbacks — named C functions, no C++ lambdas
 *
 * yield1: yield(blk, arg)        — transaction, cursor
 * yield2: yield(blk, arg1, arg2) — batch, database transaction, database cursor
 * ======================================================================== */

typedef struct { mrb_value blk; mrb_value arg; }                   mrb_lmdb_yield1_ctx;
typedef struct { mrb_value blk; mrb_value arg1; mrb_value arg2; }  mrb_lmdb_yield2_ctx;

static mrb_value
mrb_lmdb_yield1_cb(mrb_state *mrb, void *ud)
{
  mrb_lmdb_yield1_ctx *ctx = (mrb_lmdb_yield1_ctx *)ud;
  return mrb_yield(mrb, ctx->blk, ctx->arg);
}

static mrb_value
mrb_lmdb_yield2_cb(mrb_state *mrb, void *ud)
{
  mrb_lmdb_yield2_ctx *ctx = (mrb_lmdb_yield2_ctx *)ud;
  mrb_value argv[2] = { ctx->arg1, ctx->arg2 };
  return mrb_yield_argv(mrb, ctx->blk, 2, argv);
}

/* ========================================================================
 * MDB::Env
 * ======================================================================== */

static mrb_value
mrb_mdb_env_init(mrb_state *mrb, mrb_value self)
{
  mrb_value opts = mrb_nil_value();
  mrb_get_args(mrb, "|H", &opts);

  MDB_env *env;
  int rc = mdb_env_create(&env);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_env_create");
  mrb_data_init(self, env, &mdb_env_type);

  if (!mrb_nil_p(opts)) {
    mrb_value keys = mrb_hash_keys(mrb, opts);
    mrb_int n = RARRAY_LEN(keys);
    for (mrb_int i = 0; i < n; i++) {
      mrb_value k = mrb_ary_entry(keys, i);
      mrb_value v = mrb_hash_get(mrb, opts, k);

      if (!mrb_symbol_p(k))
        mrb_raisef(mrb, E_ARGUMENT_ERROR, "unknown option %v", k);

      mrb_sym sym = mrb_symbol(k);
      if (sym == MRB_SYM(mapsize)) {
        mrb_int size = mrb_integer(mrb_to_int(mrb, v));
        if (size < 0)
          mrb_raise(mrb, E_RANGE_ERROR, "mapsize must be non-negative");
        rc = mdb_env_set_mapsize(env, (size_t)size);
        if (unlikely(rc != MDB_SUCCESS))
          mrb_mdb_raise(mrb, rc, "mdb_env_set_mapsize");
      } else if (sym == MRB_SYM(maxreaders)) {
        mrb_int readers = mrb_integer(mrb_to_int(mrb, v));
        if (readers < 0 || (uint64_t)readers > UINT_MAX)
          mrb_raise(mrb, E_RANGE_ERROR, "maxreaders out of range");
        rc = mdb_env_set_maxreaders(env, (unsigned int)readers);
        if (unlikely(rc != MDB_SUCCESS))
          mrb_mdb_raise(mrb, rc, "mdb_env_set_maxreaders");
      } else if (sym == MRB_SYM(maxdbs)) {
        mrb_int dbs = mrb_integer(mrb_to_int(mrb, v));
        if (dbs < 0 || (uint64_t)dbs > UINT_MAX)
          mrb_raise(mrb, E_RANGE_ERROR, "maxdbs out of range");
        rc = mdb_env_set_maxdbs(env, (MDB_dbi)dbs);
        if (unlikely(rc != MDB_SUCCESS))
          mrb_mdb_raise(mrb, rc, "mdb_env_set_maxdbs");
      } else {
        mrb_raisef(mrb, E_ARGUMENT_ERROR, "unknown option %v", k);
      }
    }
  }
  return self;
}

static mrb_value
mrb_mdb_env_open(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  const char *path;
  mrb_int flags = 0, mode = 0600;
  mrb_get_args(mrb, "z|ii", &path, &flags, &mode);
  int rc = mdb_env_open(env, path, mrb_mdb_flags(mrb, flags), (mdb_mode_t)mode);
  if (likely(rc == MDB_SUCCESS))
    return self;
  mrb_mdb_raise(mrb, rc, "mdb_env_open");
}

static mrb_value
mrb_mdb_env_copy_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  const char *path;
  mrb_int flags = 0;
  mrb_get_args(mrb, "z|i", &path, &flags);
  int rc = flags != 0
    ? mdb_env_copy2(env, path, mrb_mdb_flags(mrb, flags))
    : mdb_env_copy(env, path);
  if (likely(rc == MDB_SUCCESS))
    return self;
  mrb_mdb_raise(mrb, rc, "mdb_env_copy");
}

static mrb_value
mrb_mdb_env_stat_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  MDB_stat stat;
  int rc = mdb_env_stat(env, &stat);
  if (likely(rc == MDB_SUCCESS))
    return mrb_mdb_stat_to_value(mrb, &stat);
  mrb_mdb_raise(mrb, rc, "mdb_env_stat");
}

static mrb_value
mrb_mdb_env_info_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  MDB_envinfo info;
  int rc = mdb_env_info(env, &info);
  if (likely(rc == MDB_SUCCESS)) {
    mrb_value args[6] = {
      info.me_mapaddr ? mrb_cptr_value(mrb, info.me_mapaddr) : mrb_nil_value(),
      mrb_convert_size_t(mrb, info.me_mapsize),
      mrb_convert_size_t(mrb, info.me_last_pgno),
      mrb_convert_size_t(mrb, info.me_last_txnid),
      mrb_convert_uint(mrb, info.me_maxreaders),
      mrb_convert_uint(mrb, info.me_numreaders),
    };
    struct RClass *env_cls  = mrb_class_get_under_id(mrb, mrb_module_get_id(mrb, MRB_SYM(MDB)), MRB_SYM(Env));
    struct RClass *info_cls = mrb_class_get_under_id(mrb, env_cls, MRB_SYM(Info));
    return mrb_obj_new(mrb, info_cls, 6, args);
  }
  mrb_mdb_raise(mrb, rc, "mdb_env_info");
}

static mrb_value
mrb_mdb_env_sync_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  mrb_bool force = FALSE;
  mrb_get_args(mrb, "|b", &force);
  int rc = mdb_env_sync(env, (int)force);
  if (likely(rc == MDB_SUCCESS))
    return self;
  mrb_mdb_raise(mrb, rc, "mdb_env_sync");
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
  if (likely(rc == MDB_SUCCESS))
    return self;
  mrb_mdb_raise(mrb, rc, "mdb_env_set_flags");
}

static mrb_value
mrb_mdb_env_get_flags_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  unsigned int flags;
  int rc = mdb_env_get_flags(env, &flags);
  if (likely(rc == MDB_SUCCESS))
    return mrb_convert_uint(mrb, flags);
  mrb_mdb_raise(mrb, rc, "mdb_env_get_flags");
}

static mrb_value
mrb_mdb_env_get_path_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  const char *path;
  int rc = mdb_env_get_path(env, &path);
  if (likely(rc == MDB_SUCCESS))
    return mrb_str_new_cstr(mrb, path);
  mrb_mdb_raise(mrb, rc, "mdb_env_get_path");
}

static mrb_value
mrb_mdb_env_set_mapsize_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  mrb_int size;
  mrb_get_args(mrb, "i", &size);
  if (size < 0)
    mrb_raise(mrb, E_RANGE_ERROR, "mapsize must be non-negative");
  int rc = mdb_env_set_mapsize(env, (size_t)size);
  if (likely(rc == MDB_SUCCESS))
    return self;
  mrb_mdb_raise(mrb, rc, "mdb_env_set_mapsize");
}

static mrb_value
mrb_mdb_env_set_maxreaders_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  mrb_int readers;
  mrb_get_args(mrb, "i", &readers);
  if (readers < 0 || (uint64_t)readers > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "maxreaders out of range");
  int rc = mdb_env_set_maxreaders(env, (unsigned int)readers);
  if (likely(rc == MDB_SUCCESS))
    return self;
  mrb_mdb_raise(mrb, rc, "mdb_env_set_maxreaders");
}

static mrb_value
mrb_mdb_env_get_maxreaders_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  unsigned int readers;
  int rc = mdb_env_get_maxreaders(env, &readers);
  if (likely(rc == MDB_SUCCESS))
    return mrb_convert_uint(mrb, readers);
  mrb_mdb_raise(mrb, rc, "mdb_env_get_maxreaders");
}

static mrb_value
mrb_mdb_env_set_maxdbs_m(mrb_state *mrb, mrb_value self)
{
  MDB_env *env = mrb_mdb_env_get(mrb, self);
  mrb_int dbs;
  mrb_get_args(mrb, "i", &dbs);
  if (dbs < 0 || (uint64_t)dbs > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "maxdbs out of range");
  int rc = mdb_env_set_maxdbs(env, (MDB_dbi)dbs);
  if (likely(rc == MDB_SUCCESS))
    return self;
  mrb_mdb_raise(mrb, rc, "mdb_env_set_maxdbs");
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
  if (likely(rc == MDB_SUCCESS))
    return mrb_convert_int(mrb, dead);
  mrb_mdb_raise(mrb, rc, "mdb_reader_check");
}

/*
 * Env#transaction([flags]) { |txn| ... }
 */
static mrb_value
mrb_mdb_env_transaction_m(mrb_state *mrb, mrb_value self)
{
  mrb_value blk;
  mrb_int flags = 0;
  mrb_get_args(mrb, "|i&!", &flags, &blk);
  if (mrb_nil_p(blk))
    mrb_raise(mrb, E_ARGUMENT_ERROR, "no block given");

  struct RClass *txn_class = mrb_class_get_under_id(mrb,
    mrb_module_get_id(mrb, MRB_SYM(MDB)), MRB_SYM(Txn));
  mrb_value argv[2] = { self, mrb_int_value(mrb, flags) };
  mrb_value txn_obj = mrb_obj_new(mrb, txn_class, 2, argv);
  mrb_gc_protect(mrb, txn_obj);

  mrb_lmdb_yield1_ctx ctx1 = { blk, txn_obj };
  mrb_bool exc = FALSE;
  mrb_value result = mrb_protect_error(mrb, mrb_lmdb_yield1_cb, &ctx1, &exc);

  MDB_txn *txn = (MDB_txn *)mrb_data_check_get_ptr(mrb, txn_obj, &mdb_txn_type);
  if (txn) {
    mrb_data_init(txn_obj, NULL, NULL);
    if (!exc) {
      int rc = mdb_txn_commit(txn);
      if (unlikely(rc != MDB_SUCCESS))
        mrb_mdb_raise(mrb, rc, "mdb_txn_commit");
    } else {
      mdb_txn_abort(txn);
    }
  }
  if (exc) mrb_exc_raise(mrb, result);
  return result;
}

/*
 * Env#database([flags[, name]]) -> MDB::Database
 *
 * Opens (or creates) a database and returns a Database object.
 * The MDB::Env object (self) is stored as @env on the Database instance
 * to prevent the GC from collecting the Env while the Database is live.
 */
static mrb_value
mrb_mdb_env_database_m(mrb_state *mrb, mrb_value self)
{
  mrb_int flags = 0;
  const char *name = NULL;
  mrb_get_args(mrb, "|iz!", &flags, &name);

  struct RClass *db_class = mrb_class_get_under_id(mrb,
    mrb_module_get_id(mrb, MRB_SYM(MDB)), MRB_SYM(Database));

  mrb_value argv[3] = { self, mrb_int_value(mrb, flags), name ? mrb_str_new_cstr(mrb, name) : mrb_nil_value() };
  return mrb_obj_new(mrb, db_class, name ? 3 : 2, argv);
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
  if (likely(rc == MDB_SUCCESS)) {
    mrb_data_init(self, txn, &mdb_txn_type);
    return self;
  }
  mrb_mdb_raise(mrb, rc, "mdb_txn_begin");
}

static mrb_value
mrb_mdb_txn_commit_m(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn = mrb_mdb_txn_get(mrb, self);
  int rc = mdb_txn_commit(txn);
  mrb_data_init(self, NULL, NULL); /* commit consumes the txn handle */
  if (likely(rc == MDB_SUCCESS))
    return mrb_true_value();
  mrb_mdb_raise(mrb, rc, "mdb_txn_commit");
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
  if (likely(rc == MDB_SUCCESS))
    return self;
  mrb_mdb_raise(mrb, rc, "mdb_txn_renew");
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
  if (likely(rc == MDB_SUCCESS))
    return mrb_convert_uint(mrb, dbi);
  mrb_mdb_raise(mrb, rc, "mdb_dbi_open");
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
  if (likely(rc == MDB_SUCCESS))
    return mrb_convert_uint(mrb, flags);
  mrb_mdb_raise(mrb, rc, "mdb_dbi_flags");
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
  MDB_val key  = { (size_t)RSTRING_LEN(key_obj), RSTRING_PTR(key_obj) };
  MDB_val data;
  int rc = mdb_get(txn, mrb_mdb_dbi(mrb, dbi), &key, &data);
  if (likely(rc == MDB_SUCCESS))
    return mrb_mdb_val_to_str(mrb, &data);
  if (rc == MDB_NOTFOUND)
    return mrb_nil_value();
  mrb_mdb_raise(mrb, rc, "mdb_get");
}

static mrb_value
mrb_mdb_put_m(mrb_state *mrb, mrb_value self)
{
  mrb_value txn_v, key_obj, data_obj;
  mrb_int dbi, flags = 0;
  mrb_get_args(mrb, "oioo|i", &txn_v, &dbi, &key_obj, &data_obj, &flags);

  MDB_txn *txn = mrb_mdb_txn_get(mrb, txn_v);
  key_obj  = mrb_str_to_str(mrb, key_obj);
  data_obj = mrb_str_to_str(mrb, data_obj);
  MDB_val key  = { (size_t)RSTRING_LEN(key_obj),  RSTRING_PTR(key_obj) };
  MDB_val data = { (size_t)RSTRING_LEN(data_obj), RSTRING_PTR(data_obj) };
  int rc = mdb_put(txn, mrb_mdb_dbi(mrb, dbi), &key, &data, mrb_mdb_flags(mrb, flags));
  if (likely(rc == MDB_SUCCESS))
    return self;
  mrb_mdb_raise(mrb, rc, "mdb_put");
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
  MDB_val dv, *dvp = NULL;
  if (!mrb_nil_p(data_obj)) {
    data_obj = mrb_str_to_str(mrb, data_obj);
    dv.mv_size = (size_t)RSTRING_LEN(data_obj);
    dv.mv_data = RSTRING_PTR(data_obj);
    dvp = &dv;
  }
  int rc = mdb_del(txn, mrb_mdb_dbi(mrb, dbi), &key, dvp);
  if (likely(rc == MDB_SUCCESS))
    return mrb_true_value();
  if (rc == MDB_NOTFOUND)
    return mrb_nil_value();
  mrb_mdb_raise(mrb, rc, "mdb_del");
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
  if (likely(rc == MDB_SUCCESS))
    return self;
  mrb_mdb_raise(mrb, rc, "mdb_drop");
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
  if (likely(rc == MDB_SUCCESS))
    return mrb_mdb_stat_to_value(mrb, &stat);
  mrb_mdb_raise(mrb, rc, "mdb_stat");
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
  if (likely(rc == MDB_SUCCESS)) {
    mrb_data_init(self, cursor, &mdb_cursor_type);
    return self;
  }
  mrb_mdb_raise(mrb, rc, "mdb_cursor_open");
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
  if (likely(rc == MDB_SUCCESS))
    return self;
  mrb_mdb_raise(mrb, rc, "mdb_cursor_renew");
}

static mrb_value
mrb_mdb_cursor_get_m(mrb_state *mrb, mrb_value self)
{
  MDB_cursor *cursor = mrb_mdb_cursor_get(mrb, self);
  mrb_int cursor_op;
  mrb_value key_obj = mrb_nil_value(), data_obj = mrb_nil_value();
  mrb_get_args(mrb, "i|oo", &cursor_op, &key_obj, &data_obj);

  MDB_val key = { 0, NULL }, data = { 0, NULL };
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
  int rc = mdb_cursor_get(cursor, &key, &data, mrb_mdb_cursor_op(mrb, cursor_op));
  if (likely(rc == MDB_SUCCESS))
    return mrb_assoc_new(mrb, mrb_mdb_val_to_str(mrb, &key), mrb_mdb_val_to_str(mrb, &data));
  if (rc == MDB_NOTFOUND)
    return mrb_nil_value();
  mrb_mdb_raise(mrb, rc, "mdb_cursor_get");
}

static mrb_value
mrb_mdb_cursor_put_m(mrb_state *mrb, mrb_value self)
{
  MDB_cursor *cursor = mrb_mdb_cursor_get(mrb, self);
  mrb_value key_obj, data_obj;
  mrb_int flags = 0;
  mrb_get_args(mrb, "oo|i", &key_obj, &data_obj, &flags);

  key_obj  = mrb_str_to_str(mrb, key_obj);
  data_obj = mrb_str_to_str(mrb, data_obj);
  MDB_val key  = { (size_t)RSTRING_LEN(key_obj),  RSTRING_PTR(key_obj) };
  MDB_val data = { (size_t)RSTRING_LEN(data_obj), RSTRING_PTR(data_obj) };
  int rc = mdb_cursor_put(cursor, &key, &data, mrb_mdb_flags(mrb, flags));
  if (likely(rc == MDB_SUCCESS))
    return self;
  mrb_mdb_raise(mrb, rc, "mdb_cursor_put");
}

static mrb_value
mrb_mdb_cursor_del_m(mrb_state *mrb, mrb_value self)
{
  MDB_cursor *cursor = mrb_mdb_cursor_get(mrb, self);
  mrb_int flags = 0;
  mrb_get_args(mrb, "|i", &flags);
  int rc = mdb_cursor_del(cursor, mrb_mdb_flags(mrb, flags));
  if (likely(rc == MDB_SUCCESS))
    return self;
  mrb_mdb_raise(mrb, rc, "mdb_cursor_del");
}

static mrb_value
mrb_mdb_cursor_count_m(mrb_state *mrb, mrb_value self)
{
  MDB_cursor *cursor = mrb_mdb_cursor_get(mrb, self);
  size_t count;
  int rc = mdb_cursor_count(cursor, &count);
  if (likely(rc == MDB_SUCCESS))
    return mrb_convert_size_t(mrb, count);
  mrb_mdb_raise(mrb, rc, "mdb_cursor_count");
}

/* ========================================================================
 * MDB::Database — all instance methods
 *
 * The Database object stores a raw MDB_env* (fast path) plus a @env ivar
 * holding the MDB::Env Ruby object to prevent GC collection of the env
 * while the database is still live.
 * ======================================================================== */

/* Database#initialize(env[, flags[, name]]) */
static mrb_value
mrb_mdb_database_init(mrb_state *mrb, mrb_value self)
{
  mrb_value env_v;
  mrb_int flags = 0;
  const char *name = NULL;
  mrb_get_args(mrb, "o|iz!", &env_v, &flags, &name);

  MDB_env *env = mrb_mdb_env_get(mrb, env_v);

  MDB_txn *txn;
  int rc = mdb_txn_begin(env, NULL, 0, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  MDB_dbi dbi;
  rc = mdb_dbi_open(txn, name, mrb_mdb_flags(mrb, flags), &dbi);
  if (unlikely(rc != MDB_SUCCESS)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_dbi_open");
  }

  /* mdb_txn_commit frees txn regardless of return value. */
  rc = mdb_txn_commit(txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_commit");

  mrb_iv_set(mrb, self, MRB_IVSYM(env), env_v);
  mrb_iv_set(mrb, self, MRB_IVSYM(dbi), mrb_convert_uint(mrb, dbi));

  return self;
}

/* Database#dbi -> Integer */
static mrb_value
mrb_mdb_database_dbi_m(mrb_state *mrb, mrb_value self)
{
  return mrb_iv_get(mrb, self, MRB_IVSYM(dbi));
}

/* Database#[] */
static mrb_value
mrb_mdb_database_aref_m(mrb_state *mrb, mrb_value self)
{
  mrb_value key_obj;
  mrb_get_args(mrb, "o", &key_obj);

  key_obj = mrb_str_to_str(mrb, key_obj);

  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, MDB_RDONLY, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  MDB_val key  = { (size_t)RSTRING_LEN(key_obj), RSTRING_PTR(key_obj) };
  MDB_val data;
  rc = mdb_get(txn, mrb_mdb_database_dbi(mrb, self), &key, &data);
  mrb_value result = (rc == MDB_SUCCESS) ? mrb_mdb_val_to_str(mrb, &data) : mrb_nil_value();
  mdb_txn_abort(txn);

  if (rc == MDB_SUCCESS || rc == MDB_NOTFOUND)
    return result;
  mrb_mdb_raise(mrb, rc, "mdb_get");
}

/* Database#[]= */
static mrb_value
mrb_mdb_database_aset_m(mrb_state *mrb, mrb_value self)
{
  mrb_value key_obj, data_obj;
  mrb_get_args(mrb, "oo", &key_obj, &data_obj);

  key_obj  = mrb_str_to_str(mrb, key_obj);
  data_obj = mrb_str_to_str(mrb, data_obj);

  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, 0, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  MDB_val key  = { (size_t)RSTRING_LEN(key_obj),  RSTRING_PTR(key_obj) };
  MDB_val data = { (size_t)RSTRING_LEN(data_obj), RSTRING_PTR(data_obj) };
  rc = mdb_put(txn, mrb_mdb_database_dbi(mrb, self), &key, &data, 0);
  if (unlikely(rc != MDB_SUCCESS)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_put");
  }
  rc = mdb_txn_commit(txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_commit");
  return data_obj;
}

/* Database#del(key[, data]) */
static mrb_value
mrb_mdb_database_del_m(mrb_state *mrb, mrb_value self)
{
  mrb_value key_obj, data_obj = mrb_nil_value();
  mrb_get_args(mrb, "o|o", &key_obj, &data_obj);

  key_obj = mrb_str_to_str(mrb, key_obj);

  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, 0, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  MDB_val key = { (size_t)RSTRING_LEN(key_obj), RSTRING_PTR(key_obj) };
  MDB_val dv, *dvp = NULL;
  if (!mrb_nil_p(data_obj)) {
    data_obj = mrb_str_to_str(mrb, data_obj);
    dv.mv_size = (size_t)RSTRING_LEN(data_obj);
    dv.mv_data = RSTRING_PTR(data_obj);
    dvp = &dv;
  }
  rc = mdb_del(txn, mrb_mdb_database_dbi(mrb, self), &key, dvp);
  if (unlikely(rc != MDB_SUCCESS && rc != MDB_NOTFOUND)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_del");
  }
  int commit_rc = mdb_txn_commit(txn);
  if (unlikely(commit_rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, commit_rc, "mdb_txn_commit");
  return self;
}

/* Database#fetch(key[, default]) { |k| ... } */
static mrb_value
mrb_mdb_database_fetch_m(mrb_state *mrb, mrb_value self)
{
  mrb_value key_obj, default_val = mrb_undef_value(), blk = mrb_nil_value();
  mrb_get_args(mrb, "o|o&", &key_obj, &default_val, &blk);

  key_obj = mrb_str_to_str(mrb, key_obj);

  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, MDB_RDONLY, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  MDB_val key  = { (size_t)RSTRING_LEN(key_obj), RSTRING_PTR(key_obj) };
  MDB_val data;
  rc = mdb_get(txn, mrb_mdb_database_dbi(mrb, self), &key, &data);
  mrb_value found_val = mrb_nil_value();
  mrb_bool found = (rc == MDB_SUCCESS);
  if (found)
    found_val = mrb_mdb_val_to_str(mrb, &data);
  mdb_txn_abort(txn);

  if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND)
    mrb_mdb_raise(mrb, rc, "mdb_get");

  if (found)
    return found_val;
  if (!mrb_nil_p(blk))
    return mrb_yield(mrb, blk, key_obj);
  if (!mrb_undef_p(default_val))
    return default_val;
  mrb_raise(mrb, E_KEY_ERROR, "key not found");
}

/* Database#stat -> MDB::Stat */
static mrb_value
mrb_mdb_database_stat_m(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, MDB_RDONLY, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");
  MDB_stat stat;
  rc = mdb_stat(txn, mrb_mdb_database_dbi(mrb, self), &stat);
  mdb_txn_abort(txn);
  if (likely(rc == MDB_SUCCESS))
    return mrb_mdb_stat_to_value(mrb, &stat);
  mrb_mdb_raise(mrb, rc, "mdb_stat");
}

/* Shared helper: open a RDONLY txn, read ms_entries, abort */
static size_t
mrb_mdb_database_entries(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, MDB_RDONLY, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");
  MDB_stat stat;
  rc = mdb_stat(txn, mrb_mdb_database_dbi(mrb, self), &stat);
  mdb_txn_abort(txn);
  if (likely(rc == MDB_SUCCESS))
    return stat.ms_entries;
  mrb_mdb_raise(mrb, rc, "mdb_stat");
}

/* Database#length / #size */
static mrb_value
mrb_mdb_database_length_m(mrb_state *mrb, mrb_value self)
{
  return mrb_convert_size_t(mrb, mrb_mdb_database_entries(mrb, self));
}

/* Database#empty? */
static mrb_value
mrb_mdb_database_empty_p_m(mrb_state *mrb, mrb_value self)
{
  return mrb_bool_value(mrb_mdb_database_entries(mrb, self) == 0);
}

/* Database#flags */
static mrb_value
mrb_mdb_database_flags_m(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, MDB_RDONLY, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");
  unsigned int flags;
  rc = mdb_dbi_flags(txn, mrb_mdb_database_dbi(mrb, self), &flags);
  mdb_txn_abort(txn);
  if (likely(rc == MDB_SUCCESS))
    return mrb_convert_uint(mrb, flags);
  mrb_mdb_raise(mrb, rc, "mdb_dbi_flags");
}

/* Database#drop(delete=false) */
static mrb_value
mrb_mdb_database_drop_m(mrb_state *mrb, mrb_value self)
{
  mrb_bool del = FALSE;
  mrb_get_args(mrb, "|b", &del);

  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, 0, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");
  rc = mdb_drop(txn, mrb_mdb_database_dbi(mrb, self), (int)del);
  if (unlikely(rc != MDB_SUCCESS)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_drop");
  }
  rc = mdb_txn_commit(txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_commit");
  return self;
}

/* Shared: open RDONLY cursor, seek to MDB_FIRST or MDB_LAST, copy pair, close */
static mrb_value
mrb_mdb_database_edge_m(mrb_state *mrb, mrb_value self, MDB_cursor_op op)
{
  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, MDB_RDONLY, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  MDB_cursor *cursor;
  rc = mdb_cursor_open(txn, mrb_mdb_database_dbi(mrb, self), &cursor);
  if (unlikely(rc != MDB_SUCCESS)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_cursor_open");
  }

  MDB_val key, data;
  rc = mdb_cursor_get(cursor, &key, &data, op);
  mrb_value result = mrb_nil_value();
  if (rc == MDB_SUCCESS)
    result = mrb_assoc_new(mrb, mrb_mdb_val_to_str(mrb, &key), mrb_mdb_val_to_str(mrb, &data));
  mdb_cursor_close(cursor);
  mdb_txn_abort(txn);

  if (rc == MDB_SUCCESS || rc == MDB_NOTFOUND)
    return result;
  mrb_mdb_raise(mrb, rc, "mdb_cursor_get");
}

/* Database#first */
static mrb_value
mrb_mdb_database_first_m(mrb_state *mrb, mrb_value self)
{
  return mrb_mdb_database_edge_m(mrb, self, MDB_FIRST);
}

/* Database#last */
static mrb_value
mrb_mdb_database_last_m(mrb_state *mrb, mrb_value self)
{
  return mrb_mdb_database_edge_m(mrb, self, MDB_LAST);
}

/*
 * Database#batch { |txn, dbi| ... }
 */
static mrb_value
mrb_mdb_database_batch_m(mrb_state *mrb, mrb_value self)
{
  mrb_value blk;
  mrb_get_args(mrb, "&!", &blk);
  if (mrb_nil_p(blk))
    mrb_raise(mrb, E_ARGUMENT_ERROR, "no block given");

  mrb_value env_obj = mrb_iv_get(mrb, self, MRB_IVSYM(env));

  struct RClass *txn_class = mrb_class_get_under_id(mrb,
    mrb_module_get_id(mrb, MRB_SYM(MDB)), MRB_SYM(Txn));
  mrb_value argv[1] = { env_obj };
  mrb_value txn_obj = mrb_obj_new(mrb, txn_class, 1, argv);
  mrb_gc_protect(mrb, txn_obj);
  mrb_value dbi_val = mrb_iv_get(mrb, self, MRB_IVSYM(dbi));

  mrb_lmdb_yield2_ctx ctx2 = { blk, txn_obj, dbi_val };
  mrb_bool exc = FALSE;
  mrb_value result = mrb_protect_error(mrb, mrb_lmdb_yield2_cb, &ctx2, &exc);

  MDB_txn *txn = (MDB_txn *)mrb_data_check_get_ptr(mrb, txn_obj, &mdb_txn_type);
  if (txn) {
    mrb_data_init(txn_obj, NULL, NULL);
    if (!exc) {
      int rc = mdb_txn_commit(txn);
      if (unlikely(rc != MDB_SUCCESS))
        mrb_mdb_raise(mrb, rc, "mdb_txn_commit");
    } else {
      mdb_txn_abort(txn);
    }
  }
  if (exc) mrb_exc_raise(mrb, result);
  return self;
}

/* Database#transaction([flags]) { |txn, dbi| ... } */
static mrb_value
mrb_mdb_database_transaction_m(mrb_state *mrb, mrb_value self)
{
  mrb_value blk;
  mrb_int flags = 0;
  mrb_get_args(mrb, "|i&!", &flags, &blk);
  if (mrb_nil_p(blk))
    mrb_raise(mrb, E_ARGUMENT_ERROR, "no block given");

  mrb_value env_obj = mrb_iv_get(mrb, self, MRB_IVSYM(env));

  struct RClass *txn_class = mrb_class_get_under_id(mrb,
    mrb_module_get_id(mrb, MRB_SYM(MDB)), MRB_SYM(Txn));
  mrb_value argv[2] = { env_obj, mrb_int_value(mrb, flags) };
  mrb_value txn_obj = mrb_obj_new(mrb, txn_class, 2, argv);
  mrb_gc_protect(mrb, txn_obj);
  mrb_value dbi_val = mrb_iv_get(mrb, self, MRB_IVSYM(dbi));

  mrb_lmdb_yield2_ctx ctx2 = { blk, txn_obj, dbi_val };
  mrb_bool exc = FALSE;
  mrb_value result = mrb_protect_error(mrb, mrb_lmdb_yield2_cb, &ctx2, &exc);

  MDB_txn *txn = (MDB_txn *)mrb_data_check_get_ptr(mrb, txn_obj, &mdb_txn_type);
  if (txn) {
    mrb_data_init(txn_obj, NULL, NULL);
    if (!exc) {
      int rc = mdb_txn_commit(txn);
      if (unlikely(rc != MDB_SUCCESS))
        mrb_mdb_raise(mrb, rc, "mdb_txn_commit");
    } else {
      mdb_txn_abort(txn);
    }
  }
  if (exc) mrb_exc_raise(mrb, result);
  return result;
}

/* Database#cursor([flags]) { |cursor| ... } */
static mrb_value
mrb_mdb_database_cursor_m(mrb_state *mrb, mrb_value self)
{
  mrb_value blk;
  mrb_int flags = 0;
  mrb_get_args(mrb, "|i&!", &flags, &blk);
  if (mrb_nil_p(blk))
    mrb_raise(mrb, E_ARGUMENT_ERROR, "no block given");

  mrb_value env_obj = mrb_iv_get(mrb, self, MRB_IVSYM(env));
  mrb_value dbi_val = mrb_iv_get(mrb, self, MRB_IVSYM(dbi));

  struct RClass *mdb_mod   = mrb_module_get_id(mrb, MRB_SYM(MDB));
  struct RClass *txn_class = mrb_class_get_under_id(mrb, mdb_mod, MRB_SYM(Txn));
  struct RClass *cur_class = mrb_class_get_under_id(mrb, mdb_mod, MRB_SYM(Cursor));

  mrb_value txn_argv[2] = { env_obj, mrb_int_value(mrb, flags) };
  mrb_value txn_obj = mrb_obj_new(mrb, txn_class, 2, txn_argv);
  mrb_gc_protect(mrb, txn_obj);

  mrb_value cur_argv[2] = { txn_obj, dbi_val };
  mrb_value cur_obj = mrb_obj_new(mrb, cur_class, 2, cur_argv);
  mrb_gc_protect(mrb, cur_obj);

  mrb_lmdb_yield1_ctx ctx1 = { blk, cur_obj };
  mrb_bool exc = FALSE;
  mrb_value result = mrb_protect_error(mrb, mrb_lmdb_yield1_cb, &ctx1, &exc);

  MDB_cursor *cursor = (MDB_cursor *)mrb_data_check_get_ptr(mrb, cur_obj, &mdb_cursor_type);
  if (cursor) {
    mrb_data_init(cur_obj, NULL, NULL);
    mdb_cursor_close(cursor);
  }

  MDB_txn *txn = (MDB_txn *)mrb_data_check_get_ptr(mrb, txn_obj, &mdb_txn_type);
  if (txn) {
    mrb_data_init(txn_obj, NULL, NULL);
    if (!exc) {
      int rc = mdb_txn_commit(txn);
      if (unlikely(rc != MDB_SUCCESS))
        mrb_mdb_raise(mrb, rc, "mdb_txn_commit");
    } else {
      mdb_txn_abort(txn);
    }
  }
  if (exc) mrb_exc_raise(mrb, result);
  return result;
}

/* Database#each { |k, v| ... } */
static mrb_value
mrb_mdb_database_each_m(mrb_state *mrb, mrb_value self)
{
  mrb_value blk;
  mrb_get_args(mrb, "&!", &blk);
  if (mrb_nil_p(blk))
    mrb_raise(mrb, E_ARGUMENT_ERROR, "no block given");


  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, MDB_RDONLY, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  MDB_cursor *cursor;
  rc = mdb_cursor_open(txn, mrb_mdb_database_dbi(mrb, self), &cursor);
  if (unlikely(rc != MDB_SUCCESS)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_cursor_open");
  }

  MDB_val key, data;
  int ai = mrb_gc_arena_save(mrb);
  mrb_value exc_val = mrb_nil_value();

  rc = mdb_cursor_get(cursor, &key, &data, MDB_FIRST);
  while (rc == MDB_SUCCESS) {
    mrb_value pair = mrb_assoc_new(mrb,
      mrb_mdb_val_to_str(mrb, &key), mrb_mdb_val_to_str(mrb, &data));
    mrb_bool exc = FALSE;
    mrb_lmdb_yield1_ctx ctx1 = { blk, pair };
    mrb_protect_error(mrb, mrb_lmdb_yield1_cb, &ctx1, &exc);
    mrb_gc_arena_restore(mrb, ai);
    if (exc) {
      exc_val = mrb_obj_value(mrb->exc);
      break;
    }
    rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT);
  }

  mdb_cursor_close(cursor);
  mdb_txn_abort(txn);

  if (!mrb_nil_p(exc_val))
    mrb_exc_raise(mrb, exc_val);
  if (rc != MDB_NOTFOUND && rc != MDB_SUCCESS)
    mrb_mdb_raise(mrb, rc, "mdb_cursor_get");
  return self;
}

/* Database#each_key(key) { |k, v| ... } — DUPSORT databases */
static mrb_value
mrb_mdb_database_each_key_m(mrb_state *mrb, mrb_value self)
{
  mrb_value key_obj, blk;
  mrb_get_args(mrb, "o&!", &key_obj, &blk);
  if (mrb_nil_p(blk))
    mrb_raise(mrb, E_ARGUMENT_ERROR, "no block given");

  key_obj = mrb_str_to_str(mrb, key_obj);

  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, MDB_RDONLY, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  MDB_cursor *cursor;
  rc = mdb_cursor_open(txn, mrb_mdb_database_dbi(mrb, self), &cursor);
  if (unlikely(rc != MDB_SUCCESS)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_cursor_open");
  }

  MDB_val key  = { (size_t)RSTRING_LEN(key_obj), RSTRING_PTR(key_obj) };
  MDB_val data;
  int ai = mrb_gc_arena_save(mrb);
  mrb_value exc_val = mrb_nil_value();

  rc = mdb_cursor_get(cursor, &key, &data, MDB_SET_KEY);
  while (rc == MDB_SUCCESS) {
    mrb_value pair = mrb_assoc_new(mrb,
      mrb_mdb_val_to_str(mrb, &key), mrb_mdb_val_to_str(mrb, &data));
    mrb_bool exc = FALSE;
    mrb_lmdb_yield1_ctx ctx1 = { blk, pair };
    mrb_protect_error(mrb, mrb_lmdb_yield1_cb, &ctx1, &exc);
    mrb_gc_arena_restore(mrb, ai);
    if (exc) {
      exc_val = mrb_obj_value(mrb->exc);
      break;
    }
    rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT_DUP);
  }

  mdb_cursor_close(cursor);
  mdb_txn_abort(txn);

  if (!mrb_nil_p(exc_val))
    mrb_exc_raise(mrb, exc_val);
  if (rc != MDB_NOTFOUND && rc != MDB_SUCCESS)
    mrb_mdb_raise(mrb, rc, "mdb_cursor_get");
  return self;
}

/* Database#each_prefix(prefix) { |k, v| ... } */
static mrb_value
mrb_mdb_database_each_prefix_m(mrb_state *mrb, mrb_value self)
{
  mrb_value prefix_obj, blk;
  mrb_get_args(mrb, "o&!", &prefix_obj, &blk);
  if (mrb_nil_p(blk))
    mrb_raise(mrb, E_ARGUMENT_ERROR, "no block given");

  prefix_obj = mrb_str_to_str(mrb, prefix_obj);

  const char *prefix_ptr = RSTRING_PTR(prefix_obj);
  mrb_int     prefix_len = RSTRING_LEN(prefix_obj);

  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, MDB_RDONLY, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  MDB_cursor *cursor;
  rc = mdb_cursor_open(txn, mrb_mdb_database_dbi(mrb, self), &cursor);
  if (unlikely(rc != MDB_SUCCESS)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_cursor_open");
  }

  MDB_val key  = { (size_t)prefix_len, (void *)prefix_ptr };
  MDB_val data;
  rc = mdb_cursor_get(cursor, &key, &data, MDB_SET_RANGE);

  int ai = mrb_gc_arena_save(mrb);
  mrb_value exc_val = mrb_nil_value();

  while (rc == MDB_SUCCESS) {
    if ((mrb_int)key.mv_size < prefix_len ||
        memcmp(key.mv_data, prefix_ptr, (size_t)prefix_len) != 0)
      break;
    mrb_value pair = mrb_assoc_new(mrb,
      mrb_mdb_val_to_str(mrb, &key), mrb_mdb_val_to_str(mrb, &data));
    mrb_bool exc = FALSE;
    mrb_lmdb_yield1_ctx ctx1 = { blk, pair };
    mrb_protect_error(mrb, mrb_lmdb_yield1_cb, &ctx1, &exc);
    mrb_gc_arena_restore(mrb, ai);
    if (exc) {
      exc_val = mrb_obj_value(mrb->exc);
      break;
    }
    rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT);
  }

  mdb_cursor_close(cursor);
  mdb_txn_abort(txn);

  if (!mrb_nil_p(exc_val))
    mrb_exc_raise(mrb, exc_val);
  if (rc != MDB_NOTFOUND && rc != MDB_SUCCESS)
    mrb_mdb_raise(mrb, rc, "mdb_cursor_get");
  return self;
}

/* Database#<< value — append with auto-increment integer key */
static mrb_value
mrb_mdb_database_append_m(mrb_state *mrb, mrb_value self)
{
  mrb_value val_obj;
  mrb_get_args(mrb, "o", &val_obj);

  val_obj = mrb_str_to_str(mrb, val_obj);

  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, 0, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  MDB_cursor *cursor;
  rc = mdb_cursor_open(txn, mrb_mdb_database_dbi(mrb, self), &cursor);
  if (unlikely(rc != MDB_SUCCESS)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_cursor_open");
  }

  MDB_val last_key, last_data;
  mrb_int next_key = 0;
  rc = mdb_cursor_get(cursor, &last_key, &last_data, MDB_LAST);
  if (rc == MDB_SUCCESS)
    next_key = mrb_lmdb_bin2fix(mrb, (const char *)last_key.mv_data, (mrb_int)last_key.mv_size) + 1;
  else if (rc != MDB_NOTFOUND) {
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_cursor_get");
  }

  mrb_value key_bin = mrb_lmdb_fix2bin(mrb, next_key);
  MDB_val key  = { (size_t)RSTRING_LEN(key_bin), RSTRING_PTR(key_bin) };
  MDB_val data = { (size_t)RSTRING_LEN(val_obj),  RSTRING_PTR(val_obj) };

  rc = mdb_cursor_put(cursor, &key, &data, MDB_APPEND);
  mdb_cursor_close(cursor);
  if (unlikely(rc != MDB_SUCCESS)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_cursor_put");
  }
  rc = mdb_txn_commit(txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_commit");
  return self;
}

/* Database#multi_get(keys) -> Array */
static mrb_value
mrb_mdb_database_multi_get_m(mrb_state *mrb, mrb_value self)
{
  mrb_value keys_ary;
  mrb_get_args(mrb, "o", &keys_ary);
  keys_ary = mrb_ensure_array_type(mrb, keys_ary);


  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, MDB_RDONLY, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  mrb_int len = RARRAY_LEN(keys_ary);
  mrb_value result = mrb_ary_new_capa(mrb, len);
  int ai = mrb_gc_arena_save(mrb);

  for (mrb_int i = 0; i < len; i++) {
    mrb_value key_obj = mrb_str_to_str(mrb, mrb_ary_entry(keys_ary, i));
    MDB_val key  = { (size_t)RSTRING_LEN(key_obj), RSTRING_PTR(key_obj) };
    MDB_val data;
    rc = mdb_get(txn, mrb_mdb_database_dbi(mrb, self), &key, &data);
    if (likely(rc == MDB_SUCCESS))
      mrb_ary_push(mrb, result, mrb_mdb_val_to_str(mrb, &data));
    else if (rc == MDB_NOTFOUND)
      mrb_ary_push(mrb, result, mrb_nil_value());
    else {
      mdb_txn_abort(txn);
      mrb_mdb_raise(mrb, rc, "mdb_get");
    }
    mrb_gc_arena_restore(mrb, ai);
  }

  mdb_txn_abort(txn);
  return result;
}

/* Database#batch_put(pairs, flags=0) */
static mrb_value
mrb_mdb_database_batch_put_m(mrb_state *mrb, mrb_value self)
{
  mrb_value pairs_ary;
  mrb_int flags = 0;
  mrb_get_args(mrb, "o|i", &pairs_ary, &flags);
  pairs_ary = mrb_ensure_array_type(mrb, pairs_ary);

  unsigned int real_flags = mrb_mdb_flags(mrb, flags);

  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, 0, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  mrb_int len = RARRAY_LEN(pairs_ary);
  int ai = mrb_gc_arena_save(mrb);

  for (mrb_int i = 0; i < len; i++) {
    mrb_value pair   = mrb_ary_entry(pairs_ary, i);
    mrb_value key_obj = mrb_str_to_str(mrb, mrb_ary_entry(pair, 0));
    mrb_value val_obj = mrb_str_to_str(mrb, mrb_ary_entry(pair, 1));
    MDB_val key  = { (size_t)RSTRING_LEN(key_obj), RSTRING_PTR(key_obj) };
    MDB_val data = { (size_t)RSTRING_LEN(val_obj), RSTRING_PTR(val_obj) };
    rc = mdb_put(txn, mrb_mdb_database_dbi(mrb, self), &key, &data, real_flags);
    if (unlikely(rc != MDB_SUCCESS)) {
      mdb_txn_abort(txn);
      mrb_mdb_raise(mrb, rc, "mdb_put");
    }
    mrb_gc_arena_restore(mrb, ai);
  }

  rc = mdb_txn_commit(txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_commit");
  return self;
}

/* Database#concat(values) — batch append with auto-increment keys */
static mrb_value
mrb_mdb_database_concat_m(mrb_state *mrb, mrb_value self)
{
  mrb_value values_ary;
  mrb_get_args(mrb, "o", &values_ary);
  values_ary = mrb_ensure_array_type(mrb, values_ary);


  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, 0, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  MDB_cursor *cursor;
  rc = mdb_cursor_open(txn, mrb_mdb_database_dbi(mrb, self), &cursor);
  if (unlikely(rc != MDB_SUCCESS)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_cursor_open");
  }

  MDB_val last_key, last_data;
  mrb_int next_key = 0;
  rc = mdb_cursor_get(cursor, &last_key, &last_data, MDB_LAST);
  if (rc == MDB_SUCCESS)
    next_key = mrb_lmdb_bin2fix(mrb, (const char *)last_key.mv_data, (mrb_int)last_key.mv_size) + 1;
  else if (rc != MDB_NOTFOUND) {
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_cursor_get");
  }

  mrb_int len = RARRAY_LEN(values_ary);
  int ai = mrb_gc_arena_save(mrb);

  for (mrb_int i = 0; i < len; i++) {
    mrb_value val_obj = mrb_str_to_str(mrb, mrb_ary_entry(values_ary, i));
    mrb_value key_bin = mrb_lmdb_fix2bin(mrb, next_key);
    MDB_val key  = { (size_t)RSTRING_LEN(key_bin), RSTRING_PTR(key_bin) };
    MDB_val data = { (size_t)RSTRING_LEN(val_obj),  RSTRING_PTR(val_obj) };
    rc = mdb_cursor_put(cursor, &key, &data, MDB_APPEND);
    if (unlikely(rc != MDB_SUCCESS)) {
      mdb_cursor_close(cursor);
      mdb_txn_abort(txn);
      mrb_mdb_raise(mrb, rc, "mdb_cursor_put");
    }
    next_key++;
    mrb_gc_arena_restore(mrb, ai);
  }

  mdb_cursor_close(cursor);
  rc = mdb_txn_commit(txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_commit");
  return self;
}

/* Database#to_a */
static mrb_value
mrb_mdb_database_to_a_m(mrb_state *mrb, mrb_value self)
{

  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, MDB_RDONLY, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  unsigned int db_flags;
  rc = mdb_dbi_flags(txn, mrb_mdb_database_dbi(mrb, self), &db_flags);
  if (unlikely(rc != MDB_SUCCESS)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_dbi_flags");
  }

  MDB_cursor *cursor;
  rc = mdb_cursor_open(txn, mrb_mdb_database_dbi(mrb, self), &cursor);
  if (unlikely(rc != MDB_SUCCESS)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_cursor_open");
  }

  mrb_value ary = mrb_ary_new(mrb);
  int ai = mrb_gc_arena_save(mrb);

  if (db_flags & MDB_DUPSORT) {
    MDB_val key, data;
    rc = mdb_cursor_get(cursor, &key, &data, MDB_FIRST);
    while (rc == MDB_SUCCESS) {
      int rc2 = mdb_cursor_get(cursor, &key, &data, MDB_FIRST_DUP);
      while (rc2 == MDB_SUCCESS) {
        mrb_ary_push(mrb, ary,
          mrb_assoc_new(mrb, mrb_mdb_val_to_str(mrb, &key), mrb_mdb_val_to_str(mrb, &data)));
        mrb_gc_arena_restore(mrb, ai);
        rc2 = mdb_cursor_get(cursor, &key, &data, MDB_NEXT_DUP);
      }
      rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT_NODUP);
    }
  } else {
    MDB_val key, data;
    rc = mdb_cursor_get(cursor, &key, &data, MDB_FIRST);
    while (rc == MDB_SUCCESS) {
      mrb_ary_push(mrb, ary,
        mrb_assoc_new(mrb, mrb_mdb_val_to_str(mrb, &key), mrb_mdb_val_to_str(mrb, &data)));
      mrb_gc_arena_restore(mrb, ai);
      rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT);
    }
  }

  mdb_cursor_close(cursor);
  mdb_txn_abort(txn);

  if (rc != MDB_NOTFOUND)
    mrb_mdb_raise(mrb, rc, "mdb_cursor_get");
  return ary;
}

/* Database#to_h */
static mrb_value
mrb_mdb_database_to_h_m(mrb_state *mrb, mrb_value self)
{

  MDB_txn *txn;
  int rc = mdb_txn_begin(mrb_mdb_database_env(mrb, self), NULL, MDB_RDONLY, &txn);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_txn_begin");

  unsigned int db_flags;
  rc = mdb_dbi_flags(txn, mrb_mdb_database_dbi(mrb, self), &db_flags);
  if (unlikely(rc != MDB_SUCCESS)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_dbi_flags");
  }

  MDB_cursor *cursor;
  rc = mdb_cursor_open(txn, mrb_mdb_database_dbi(mrb, self), &cursor);
  if (unlikely(rc != MDB_SUCCESS)) {
    mdb_txn_abort(txn);
    mrb_mdb_raise(mrb, rc, "mdb_cursor_open");
  }

  mrb_value hsh = mrb_hash_new(mrb);
  int ai = mrb_gc_arena_save(mrb);

  if (db_flags & MDB_DUPSORT) {
    MDB_val key, data;
    rc = mdb_cursor_get(cursor, &key, &data, MDB_FIRST);
    while (rc == MDB_SUCCESS) {
      mrb_value k      = mrb_mdb_val_to_str(mrb, &key);
      mrb_value bucket = mrb_hash_fetch(mrb, hsh, k, mrb_nil_value());
      if (mrb_nil_p(bucket)) {
        bucket = mrb_ary_new(mrb);
        mrb_hash_set(mrb, hsh, k, bucket);
      }
      int rc2 = mdb_cursor_get(cursor, &key, &data, MDB_FIRST_DUP);
      while (rc2 == MDB_SUCCESS) {
        mrb_ary_push(mrb, bucket, mrb_mdb_val_to_str(mrb, &data));
        mrb_gc_arena_restore(mrb, ai);
        rc2 = mdb_cursor_get(cursor, &key, &data, MDB_NEXT_DUP);
      }
      rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT_NODUP);
    }
  } else {
    MDB_val key, data;
    rc = mdb_cursor_get(cursor, &key, &data, MDB_FIRST);
    while (rc == MDB_SUCCESS) {
      mrb_hash_set(mrb, hsh,
        mrb_mdb_val_to_str(mrb, &key), mrb_mdb_val_to_str(mrb, &data));
      mrb_gc_arena_restore(mrb, ai);
      rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT);
    }
  }

  mdb_cursor_close(cursor);
  mdb_txn_abort(txn);

  if (rc != MDB_NOTFOUND)
    mrb_mdb_raise(mrb, rc, "mdb_cursor_get");
  return hsh;
}

/* ========================================================================
 * MDB bulk module functions (low-level, used by old Ruby helpers still
 * callable from user code via MDB.get / MDB.put / MDB.del etc.)
 * ======================================================================== */

static mrb_value
mrb_mdb_multi_get_m(mrb_state *mrb, mrb_value self)
{
  mrb_value txn_v;
  const mrb_value *keys;
  mrb_int dbi, len;
  mrb_get_args(mrb, "oia", &txn_v, &dbi, &keys, &len);

  MDB_txn *txn = mrb_mdb_txn_get(mrb, txn_v);
  MDB_dbi real_dbi = mrb_mdb_dbi(mrb, dbi);
  mrb_value result = mrb_ary_new_capa(mrb, len);
  int ai = mrb_gc_arena_save(mrb);

  for (mrb_int i = 0; i < len; i++) {
    mrb_value key_obj = mrb_str_to_str(mrb, keys[i]);
    MDB_val key  = { (size_t)RSTRING_LEN(key_obj), RSTRING_PTR(key_obj) };
    MDB_val data;
    int rc = mdb_get(txn, real_dbi, &key, &data);
    if (likely(rc == MDB_SUCCESS))
      mrb_ary_push(mrb, result, mrb_mdb_val_to_str(mrb, &data));
    else if (rc == MDB_NOTFOUND)
      mrb_ary_push(mrb, result, mrb_nil_value());
    else
      mrb_mdb_raise(mrb, rc, "mdb_get");
    mrb_gc_arena_restore(mrb, ai);
  }
  return result;
}

static mrb_value
mrb_mdb_batch_put_m(mrb_state *mrb, mrb_value self)
{
  mrb_value txn_v;
  const mrb_value *pairs;
  mrb_int dbi, len, flags = 0;
  mrb_get_args(mrb, "oia|i", &txn_v, &dbi, &pairs, &len, &flags);

  MDB_txn *txn = mrb_mdb_txn_get(mrb, txn_v);
  MDB_dbi real_dbi = mrb_mdb_dbi(mrb, dbi);
  unsigned int real_flags = mrb_mdb_flags(mrb, flags);
  int ai = mrb_gc_arena_save(mrb);

  for (mrb_int i = 0; i < len; i++) {
    mrb_value key_obj = mrb_str_to_str(mrb, mrb_ary_entry(pairs[i], 0));
    mrb_value val_obj = mrb_str_to_str(mrb, mrb_ary_entry(pairs[i], 1));
    MDB_val key  = { (size_t)RSTRING_LEN(key_obj), RSTRING_PTR(key_obj) };
    MDB_val data = { (size_t)RSTRING_LEN(val_obj), RSTRING_PTR(val_obj) };
    int rc = mdb_put(txn, real_dbi, &key, &data, real_flags);
    if (likely(rc == MDB_SUCCESS)) { mrb_gc_arena_restore(mrb, ai); continue; }
    mrb_mdb_raise(mrb, rc, "mdb_put");
  }
  return self;
}

static mrb_value
mrb_mdb_append_values_m(mrb_state *mrb, mrb_value self)
{
  mrb_value txn_v;
  const mrb_value *values;
  mrb_int dbi, len;
  mrb_get_args(mrb, "oia", &txn_v, &dbi, &values, &len);

  MDB_txn *txn = mrb_mdb_txn_get(mrb, txn_v);
  MDB_dbi real_dbi = mrb_mdb_dbi(mrb, dbi);

  MDB_cursor *cursor;
  int rc = mdb_cursor_open(txn, real_dbi, &cursor);
  if (unlikely(rc != MDB_SUCCESS))
    mrb_mdb_raise(mrb, rc, "mdb_cursor_open");

  MDB_val last_key, last_data;
  mrb_int next_key = 0;
  rc = mdb_cursor_get(cursor, &last_key, &last_data, MDB_LAST);
  if (rc == MDB_SUCCESS)
    next_key = mrb_lmdb_bin2fix(mrb, (const char *)last_key.mv_data, (mrb_int)last_key.mv_size) + 1;
  else if (rc != MDB_NOTFOUND) {
    mdb_cursor_close(cursor);
    mrb_mdb_raise(mrb, rc, "mdb_cursor_get");
  }

  int ai = mrb_gc_arena_save(mrb);
  for (mrb_int i = 0; i < len; i++) {
    mrb_value val_obj = mrb_str_to_str(mrb, values[i]);
    mrb_value key_obj = mrb_lmdb_fix2bin(mrb, next_key);
    MDB_val key  = { (size_t)RSTRING_LEN(key_obj), RSTRING_PTR(key_obj) };
    MDB_val data = { (size_t)RSTRING_LEN(val_obj), RSTRING_PTR(val_obj) };
    rc = mdb_cursor_put(cursor, &key, &data, MDB_APPEND);
    if (likely(rc == MDB_SUCCESS)) { next_key++; mrb_gc_arena_restore(mrb, ai); continue; }
    mdb_cursor_close(cursor);
    mrb_mdb_raise(mrb, rc, "mdb_cursor_put");
  }
  mdb_cursor_close(cursor);
  return mrb_int_value(mrb, len);
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
  if (likely(rc == MDB_SUCCESS))
    return mrb_str_new(mrb, (const char *)data.mv_data, (mrb_int)data.mv_size);
  if (rc == MDB_NOTFOUND)
    return mrb_nil_value();
  mrb_mdb_raise(mrb, rc, "mdb_get");
}

MRB_API void
mrb_lmdb_put(mrb_state *mrb, MDB_txn *txn, MDB_dbi dbi,
             const void *key_data, size_t key_len,
             const void *val_data, size_t val_len,
             unsigned int flags)
{
  MDB_val key  = { key_len, (void *)key_data };
  MDB_val data = { val_len, (void *)val_data };
  int rc = mdb_put(txn, dbi, &key, &data, flags);
  if (likely(rc == MDB_SUCCESS))
    return;
  mrb_mdb_raise(mrb, rc, "mdb_put");
}

MRB_API mrb_bool
mrb_lmdb_del(mrb_state *mrb, MDB_txn *txn, MDB_dbi dbi,
             const void *key_data, size_t key_len)
{
  MDB_val key = { key_len, (void *)key_data };
  int rc = mdb_del(txn, dbi, &key, NULL);
  if (likely(rc == MDB_SUCCESS))
    return TRUE;
  if (rc == MDB_NOTFOUND)
    return FALSE;
  mrb_mdb_raise(mrb, rc, "mdb_del");
}

/* ========================================================================
 * Gem init / final
 * ======================================================================== */

MRB_API void
mrb_mruby_lmdb_gem_init(mrb_state *mrb)
{
  struct RClass *mdb_mod          = mrb_define_module_id(mrb, MRB_SYM(MDB));
  struct RClass *mdb_error_class;
  struct RClass *mdb_env_class;
  struct RClass *mdb_txn_class;
  struct RClass *mdb_cursor_class;
  struct RClass *mdb_dbi_mod;
  struct RClass *mdb_database_class;

  mrb_define_const_id(mrb, mdb_mod, MRB_SYM(VERSION),
    mrb_str_new_lit_frozen(mrb, MDB_VERSION_STRING));

  /* ── Constants ───────────────────────────────────────────────────────── */
  #define DEFINE_MDB_CONST(name) \
    mrb_define_const_id(mrb, mdb_mod, MRB_SYM(name), mrb_int_value(mrb, MDB_##name))

  DEFINE_MDB_CONST(FIXEDMAP);   DEFINE_MDB_CONST(NOSUBDIR);
  DEFINE_MDB_CONST(NOSYNC);     DEFINE_MDB_CONST(RDONLY);
  DEFINE_MDB_CONST(NOMETASYNC); DEFINE_MDB_CONST(WRITEMAP);
  DEFINE_MDB_CONST(MAPASYNC);   DEFINE_MDB_CONST(NOTLS);
  DEFINE_MDB_CONST(NOLOCK);     DEFINE_MDB_CONST(NORDAHEAD);
  DEFINE_MDB_CONST(NOMEMINIT);  DEFINE_MDB_CONST(REVERSEKEY);
  DEFINE_MDB_CONST(DUPSORT);    DEFINE_MDB_CONST(INTEGERKEY);
  DEFINE_MDB_CONST(DUPFIXED);   DEFINE_MDB_CONST(INTEGERDUP);
  DEFINE_MDB_CONST(REVERSEDUP); DEFINE_MDB_CONST(CREATE);
  DEFINE_MDB_CONST(NOOVERWRITE);DEFINE_MDB_CONST(NODUPDATA);
  DEFINE_MDB_CONST(CURRENT);    DEFINE_MDB_CONST(RESERVE);
  DEFINE_MDB_CONST(APPEND);     DEFINE_MDB_CONST(APPENDDUP);
  DEFINE_MDB_CONST(MULTIPLE);   DEFINE_MDB_CONST(CP_COMPACT);
  #undef DEFINE_MDB_CONST

  /* ── MDB::Error ──────────────────────────────────────────────────────── */
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

  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(initialize),   mrb_mdb_env_init,             MRB_ARGS_OPT(1));
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(open),         mrb_mdb_env_open,             MRB_ARGS_ARG(1,2));
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(copy),         mrb_mdb_env_copy_m,           MRB_ARGS_ARG(1,1));
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(stat),         mrb_mdb_env_stat_m,           MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(info),         mrb_mdb_env_info_m,           MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(sync),         mrb_mdb_env_sync_m,           MRB_ARGS_OPT(1));
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(close),        mrb_mdb_env_close_m,          MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(set_flags),    mrb_mdb_env_set_flags_m,      MRB_ARGS_OPT(2));
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(flags),        mrb_mdb_env_get_flags_m,      MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(path),         mrb_mdb_env_get_path_m,       MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM_E(mapsize),    mrb_mdb_env_set_mapsize_m,    MRB_ARGS_REQ(1));
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM_E(maxreaders), mrb_mdb_env_set_maxreaders_m, MRB_ARGS_REQ(1));
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(maxreaders),   mrb_mdb_env_get_maxreaders_m, MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM_E(maxdbs),     mrb_mdb_env_set_maxdbs_m,     MRB_ARGS_REQ(1));
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(maxkeysize),   mrb_mdb_env_get_maxkeysize_m, MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(reader_check), mrb_mdb_reader_check_m,       MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(transaction),  mrb_mdb_env_transaction_m,    MRB_ARGS_OPT(1)|MRB_ARGS_BLOCK());
  mrb_define_method_id(mrb, mdb_env_class,       MRB_SYM(database),     mrb_mdb_env_database_m,       MRB_ARGS_OPT(2));

  /* ── MDB::Txn ────────────────────────────────────────────────────────── */
  mdb_txn_class = mrb_define_class_under_id(mrb, mdb_mod,
    MRB_SYM(Txn), mrb->object_class);
  MRB_SET_INSTANCE_TT(mdb_txn_class, MRB_TT_CDATA);

  mrb_define_method_id(mrb, mdb_txn_class, MRB_SYM(initialize), mrb_mdb_txn_init,     MRB_ARGS_ARG(1,2));
  mrb_define_method_id(mrb, mdb_txn_class, MRB_SYM(commit),     mrb_mdb_txn_commit_m, MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_txn_class, MRB_SYM(abort),      mrb_mdb_txn_abort_m,  MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_txn_class, MRB_SYM(reset),      mrb_mdb_txn_reset_m,  MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_txn_class, MRB_SYM(renew),      mrb_mdb_txn_renew_m,  MRB_ARGS_NONE());

  /* ── MDB::Dbi ────────────────────────────────────────────────────────── */
  mdb_dbi_mod = mrb_define_module_under_id(mrb, mdb_mod, MRB_SYM(Dbi));
  mrb_define_module_function_id(mrb, mdb_dbi_mod, MRB_SYM(open),  mrb_mdb_dbi_open_m,  MRB_ARGS_ARG(1,2));
  mrb_define_module_function_id(mrb, mdb_dbi_mod, MRB_SYM(flags), mrb_mdb_dbi_flags_m, MRB_ARGS_REQ(2));

  /* ── MDB module functions ────────────────────────────────────────────── */
  mrb_define_module_function_id(mrb, mdb_mod, MRB_SYM(stat),          mrb_mdb_stat_m,          MRB_ARGS_REQ(2));
  mrb_define_module_function_id(mrb, mdb_mod, MRB_SYM(get),           mrb_mdb_get_m,           MRB_ARGS_REQ(3));
  mrb_define_module_function_id(mrb, mdb_mod, MRB_SYM(put),           mrb_mdb_put_m,           MRB_ARGS_ARG(4,1));
  mrb_define_module_function_id(mrb, mdb_mod, MRB_SYM(del),           mrb_mdb_del_m,           MRB_ARGS_ARG(3,1));
  mrb_define_module_function_id(mrb, mdb_mod, MRB_SYM(drop),          mrb_mdb_drop_m,          MRB_ARGS_ARG(2,1));
  mrb_define_module_function_id(mrb, mdb_mod, MRB_SYM(multi_get),     mrb_mdb_multi_get_m,     MRB_ARGS_REQ(3));
  mrb_define_module_function_id(mrb, mdb_mod, MRB_SYM(batch_put),     mrb_mdb_batch_put_m,     MRB_ARGS_ARG(3,1));
  mrb_define_module_function_id(mrb, mdb_mod, MRB_SYM(append_values), mrb_mdb_append_values_m, MRB_ARGS_REQ(3));

  /* ── MDB::Cursor ─────────────────────────────────────────────────────── */
  mdb_cursor_class = mrb_define_class_under_id(mrb, mdb_mod,
    MRB_SYM(Cursor), mrb->object_class);
  MRB_SET_INSTANCE_TT(mdb_cursor_class, MRB_TT_CDATA);

  #define DEFINE_CURSOR_CONST(name) \
    mrb_define_const_id(mrb, mdb_cursor_class, MRB_SYM(name), mrb_int_value(mrb, MDB_##name))
  DEFINE_CURSOR_CONST(FIRST);       DEFINE_CURSOR_CONST(FIRST_DUP);
  DEFINE_CURSOR_CONST(GET_BOTH);    DEFINE_CURSOR_CONST(GET_BOTH_RANGE);
  DEFINE_CURSOR_CONST(GET_CURRENT); DEFINE_CURSOR_CONST(GET_MULTIPLE);
  DEFINE_CURSOR_CONST(LAST);        DEFINE_CURSOR_CONST(LAST_DUP);
  DEFINE_CURSOR_CONST(NEXT);        DEFINE_CURSOR_CONST(NEXT_DUP);
  DEFINE_CURSOR_CONST(NEXT_MULTIPLE);DEFINE_CURSOR_CONST(NEXT_NODUP);
  DEFINE_CURSOR_CONST(PREV);        DEFINE_CURSOR_CONST(PREV_DUP);
  DEFINE_CURSOR_CONST(PREV_NODUP);
  DEFINE_CURSOR_CONST(SET);         DEFINE_CURSOR_CONST(SET_KEY);
  DEFINE_CURSOR_CONST(SET_RANGE);   DEFINE_CURSOR_CONST(PREV_MULTIPLE);
  #undef DEFINE_CURSOR_CONST

  mrb_define_method_id(mrb, mdb_cursor_class, MRB_SYM(initialize), mrb_mdb_cursor_init,    MRB_ARGS_REQ(2));
  mrb_define_method_id(mrb, mdb_cursor_class, MRB_SYM(close),      mrb_mdb_cursor_close_m, MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_cursor_class, MRB_SYM(renew),      mrb_mdb_cursor_renew_m, MRB_ARGS_REQ(1));
  mrb_define_method_id(mrb, mdb_cursor_class, MRB_SYM(get),        mrb_mdb_cursor_get_m,   MRB_ARGS_ARG(1,2));
  mrb_define_method_id(mrb, mdb_cursor_class, MRB_SYM(put),        mrb_mdb_cursor_put_m,   MRB_ARGS_ARG(2,1));
  mrb_define_method_id(mrb, mdb_cursor_class, MRB_SYM(del),        mrb_mdb_cursor_del_m,   MRB_ARGS_OPT(1));
  mrb_define_method_id(mrb, mdb_cursor_class, MRB_SYM(count),      mrb_mdb_cursor_count_m, MRB_ARGS_NONE());

  /* Cursor::Ops hash — used by Ruby mrblib to define named cursor methods */
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

  /* ── MDB::Database ───────────────────────────────────────────────────── */
  mdb_database_class = mrb_define_class_under_id(mrb, mdb_mod,
    MRB_SYM(Database), mrb->object_class);

  mrb_include_module(mrb, mdb_database_class,
    mrb_module_get_id(mrb, MRB_SYM(Enumerable)));

  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(initialize),  mrb_mdb_database_init,        MRB_ARGS_ARG(1,2));
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(dbi),         mrb_mdb_database_dbi_m,       MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_database_class, MRB_OPSYM(aref),          mrb_mdb_database_aref_m,      MRB_ARGS_REQ(1));
  mrb_define_method_id(mrb, mdb_database_class, MRB_OPSYM(aset),        mrb_mdb_database_aset_m,      MRB_ARGS_REQ(2));
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(del),         mrb_mdb_database_del_m,       MRB_ARGS_ARG(1,1));
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(fetch),       mrb_mdb_database_fetch_m,     MRB_ARGS_ARG(1,1)|MRB_ARGS_BLOCK());
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(stat),        mrb_mdb_database_stat_m,      MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(length),      mrb_mdb_database_length_m,    MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(size),        mrb_mdb_database_length_m,    MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM_Q(empty),     mrb_mdb_database_empty_p_m,   MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(flags),       mrb_mdb_database_flags_m,     MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(drop),        mrb_mdb_database_drop_m,      MRB_ARGS_OPT(1));
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(first),       mrb_mdb_database_first_m,     MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(last),        mrb_mdb_database_last_m,      MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(batch),       mrb_mdb_database_batch_m,       MRB_ARGS_BLOCK());
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(transaction), mrb_mdb_database_transaction_m, MRB_ARGS_OPT(1)|MRB_ARGS_BLOCK());
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(cursor),      mrb_mdb_database_cursor_m,      MRB_ARGS_OPT(1)|MRB_ARGS_BLOCK());
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(each),        mrb_mdb_database_each_m,      MRB_ARGS_BLOCK());
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(each_key),    mrb_mdb_database_each_key_m,  MRB_ARGS_REQ(1)|MRB_ARGS_BLOCK());
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(each_prefix), mrb_mdb_database_each_prefix_m, MRB_ARGS_REQ(1)|MRB_ARGS_BLOCK());
  mrb_define_method_id(mrb, mdb_database_class, MRB_OPSYM(lshift),          mrb_mdb_database_append_m,    MRB_ARGS_REQ(1));
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(multi_get),   mrb_mdb_database_multi_get_m, MRB_ARGS_REQ(1));
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(batch_put),   mrb_mdb_database_batch_put_m, MRB_ARGS_ARG(1,1));
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(concat),      mrb_mdb_database_concat_m,    MRB_ARGS_REQ(1));
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(to_a),        mrb_mdb_database_to_a_m,      MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mdb_database_class, MRB_SYM(to_h),        mrb_mdb_database_to_h_m,      MRB_ARGS_NONE());

  /* ── Integer#to_bin, String#to_fix ──────────────────────────────────── */
  mrb_define_method_id(mrb, mrb->string_class,  MRB_SYM(to_fix), mrb_bin2fix_m, MRB_ARGS_NONE());
  mrb_define_method_id(mrb, mrb->integer_class, MRB_SYM(to_bin), mrb_fix2bin_m, MRB_ARGS_NONE());
}

MRB_API void
mrb_mruby_lmdb_gem_final(mrb_state *mrb)
{
  (void)mrb;
}