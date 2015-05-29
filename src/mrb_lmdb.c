#include "mruby/lmdb.h"
#include "mrb_lmdb.h"

static void
mrb_mdb_env_close(mrb_state *mrb, void *p)
{
  mdb_env_close((MDB_env *) p);
}

static const struct mrb_data_type mdb_env_type = {
  "$mrb_i_mdb_env", mrb_mdb_env_close,
};

static mrb_value
mrb_mdb_env_create(mrb_state *mrb, mrb_value self)
{
  MDB_env *env;

  int rc = mdb_env_create (&env);

  if (rc != 0)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  mrb_data_init(self, env, &mdb_env_type);

  return self;
}

static mrb_value
mrb_mdb_env_open(mrb_state *mrb, mrb_value self)
{
  char *path;
  mrb_int flags = 0, mode = 0600;

  mrb_get_args(mrb, "z|ii", &path, &flags, &mode);

  if (flags < 0 ||flags > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");

  if (mode < INT_MIN ||mode > INT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "mode is out of range");

  int rc = mdb_env_open((MDB_env *) DATA_PTR(self), (const char *) path,
    (unsigned int) flags, (mdb_mode_t) mode);

  if (rc != 0)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

static void
mrb_mdb_txn_free(mrb_state *mrb, void *p)
{
  mdb_txn_abort((MDB_txn *) p);
}

static const struct mrb_data_type mdb_txn_type = {
  "$mrb_i_mdb_txn", mrb_mdb_txn_free,
};

static mrb_value
mrb_mdb_txn_begin(mrb_state *mrb, mrb_value self)
{
  MDB_env *env;
  MDB_txn *parent = NULL;
  mrb_int flags = 0;
  MDB_txn *txn;

  mrb_get_args (mrb, "d|id", &env, &mdb_env_type, &flags, &parent, &mdb_txn_type);

  if (flags < 0 ||flags > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");

  int rc = mdb_txn_begin(env, parent, (unsigned int) flags, &txn);

  if (rc != 0)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  mrb_data_init(self, txn, &mdb_txn_type);

  return self;
}

static mrb_value
mrb_mdb_txn_commit(mrb_state *mrb, mrb_value self)
{
  int rc = mdb_txn_commit((MDB_txn *) DATA_PTR(self));

  if (rc != 0)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return mrb_true_value();
}

static mrb_value
mrb_mdb_txn_abort(mrb_state *mrb, mrb_value self)
{
  mdb_txn_abort((MDB_txn *) DATA_PTR(self));

  return mrb_true_value();
}

static mrb_value
mrb_mdb_txn_reset(mrb_state *mrb, mrb_value self)
{
  mdb_txn_reset((MDB_txn *) DATA_PTR(self));

  return self;
}

static mrb_value
mrb_mdb_txn_renew(mrb_state *mrb, mrb_value self)
{
  int rc = mdb_txn_renew((MDB_txn *) DATA_PTR(self));

  if (rc != 0)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

static mrb_value
mrb_mdb_dbi_open(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn;
  mrb_int flags = MDB_CREATE;
  char *name = NULL;
  MDB_dbi dbi;

  mrb_get_args(mrb, "d|iz", &txn, &mdb_txn_type, &flags, &name);

  if (flags < 0 ||flags > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");

  int rc = mdb_dbi_open(txn, (const char *) name, (unsigned int) flags, &dbi);

  if (rc != 0)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  if (dbi > MRB_INT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");

  return mrb_fixnum_value(dbi);
}

static mrb_value
mrb_mdb_drop(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn;
  mrb_int dbi;
  mrb_bool del = FALSE;

  mrb_get_args(mrb, "di|b", &txn, &mdb_txn_type, &dbi, &del);

  int rc = mdb_drop(txn, (MDB_dbi) dbi, (int) del);

  if (rc != 0)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return mrb_true_value();
}

static mrb_value
mrb_mdb_get(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn;
  mrb_int dbi;
  mrb_value key_obj;
  mrb_bool static_string = FALSE;
  MDB_val key, data;

  mrb_get_args(mrb, "dio|b", &txn, &mdb_txn_type, &dbi, &key_obj, &static_string);

  if (dbi < 0 ||dbi > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");

  key_obj = mrb_str_to_str(mrb, key_obj);

  key.mv_size = RSTRING_LEN(key_obj);
  key.mv_data = RSTRING_PTR(key_obj);

  int rc = mdb_get(txn, (MDB_dbi) dbi, &key, &data);

  if (rc == 0) {
    if (static_string)
      return mrb_str_new_static(mrb, (const char *) data.mv_data, data.mv_size);
    else
      return mrb_str_new(mrb, (const char *) data.mv_data, data.mv_size);
  }
  else
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));
}

static mrb_value
mrb_mdb_put(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn;
  mrb_int dbi;
  mrb_value key_obj, data_obj;
  mrb_int flags = 0;
  MDB_val key;
  MDB_val data;

  mrb_get_args(mrb, "dioo|i", &txn, &mdb_txn_type, &dbi, &key_obj, &data_obj, &flags);

  if (dbi < 0 ||dbi > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");

  if (flags < 0 ||flags > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");

  key_obj = mrb_str_to_str(mrb, key_obj);
  data_obj = mrb_str_to_str(mrb, data_obj);

  key.mv_size = RSTRING_LEN(key_obj);
  key.mv_data = RSTRING_PTR(key_obj);
  data.mv_size = RSTRING_LEN(data_obj);
  data.mv_data = RSTRING_PTR(data_obj);

  int rc = mdb_put(txn, (MDB_dbi) dbi, &key, &data, (unsigned int) flags);

  if(rc != 0)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

void
mrb_mruby_lmdb_gem_init(mrb_state* mrb) {
  struct RClass *mdb_mod, *mdb_env_class, *mdb_txn_class, *mdb_dbi_mod;

  mdb_mod = mrb_define_module(mrb, "MDB");
  mrb_define_const(mrb, mdb_mod,  "FIXEDMAP",     mrb_fixnum_value(MDB_FIXEDMAP));
  mrb_define_const(mrb, mdb_mod,  "NOSUBDIR",     mrb_fixnum_value(MDB_NOSUBDIR));
  mrb_define_const(mrb, mdb_mod,  "NOSYNC",       mrb_fixnum_value(MDB_NOSYNC));
  mrb_define_const(mrb, mdb_mod,  "RDONLY",       mrb_fixnum_value(MDB_RDONLY));
  mrb_define_const(mrb, mdb_mod,  "NOMETASYNC",   mrb_fixnum_value(MDB_NOMETASYNC));
  mrb_define_const(mrb, mdb_mod,  "WRITEMAP",     mrb_fixnum_value(MDB_WRITEMAP));
  mrb_define_const(mrb, mdb_mod,  "MAPASYNC",     mrb_fixnum_value(MDB_MAPASYNC));
  mrb_define_const(mrb, mdb_mod,  "NOTLS",        mrb_fixnum_value(MDB_NOTLS));
  mrb_define_const(mrb, mdb_mod,  "NOLOCK",       mrb_fixnum_value(MDB_NOLOCK));
  mrb_define_const(mrb, mdb_mod,  "NORDAHEAD",    mrb_fixnum_value(MDB_NORDAHEAD));
  mrb_define_const(mrb, mdb_mod,  "NOMEMINIT",    mrb_fixnum_value(MDB_NOMEMINIT));
  mrb_define_const(mrb, mdb_mod,  "REVERSEKEY",   mrb_fixnum_value(MDB_REVERSEKEY));
  mrb_define_const(mrb, mdb_mod,  "DUPSORT",      mrb_fixnum_value(MDB_DUPSORT));
  mrb_define_const(mrb, mdb_mod,  "INTEGERKEY",   mrb_fixnum_value(MDB_INTEGERKEY));
  mrb_define_const(mrb, mdb_mod,  "DUPFIXED",     mrb_fixnum_value(MDB_DUPFIXED));
  mrb_define_const(mrb, mdb_mod,  "INTEGERDUP",   mrb_fixnum_value(MDB_INTEGERDUP));
  mrb_define_const(mrb, mdb_mod,  "REVERSEDUP",   mrb_fixnum_value(MDB_REVERSEDUP));
  mrb_define_const(mrb, mdb_mod,  "CREATE",       mrb_fixnum_value(MDB_CREATE));
  mrb_define_const(mrb, mdb_mod,  "NOOVERWRITE",  mrb_fixnum_value(MDB_NOOVERWRITE));
  mrb_define_const(mrb, mdb_mod,  "NODUPDATA",    mrb_fixnum_value(MDB_NODUPDATA));
  mrb_define_const(mrb, mdb_mod,  "CURRENT",      mrb_fixnum_value(MDB_CURRENT));
  mrb_define_const(mrb, mdb_mod,  "RESERVE",      mrb_fixnum_value(MDB_RESERVE));
  mrb_define_const(mrb, mdb_mod,  "APPEND",       mrb_fixnum_value(MDB_APPEND));
  mrb_define_const(mrb, mdb_mod,  "APPENDDUP",    mrb_fixnum_value(MDB_APPENDDUP));
  mrb_define_const(mrb, mdb_mod,  "MULTIPLE",     mrb_fixnum_value(MDB_MULTIPLE));
  mrb_define_const(mrb, mdb_mod,  "CP_COMPACT",   mrb_fixnum_value(MDB_CP_COMPACT));
  mrb_define_class_under(mrb, mdb_mod, "Error", E_RUNTIME_ERROR);
  mdb_env_class = mrb_define_class_under(mrb, mdb_mod, "Env", mrb->object_class);
  MRB_SET_INSTANCE_TT(mdb_env_class, MRB_TT_DATA);
  mrb_define_method(mrb, mdb_env_class, "initialize", mrb_mdb_env_create, MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_env_class, "open", mrb_mdb_env_open, MRB_ARGS_ARG(1, 2));
  mdb_txn_class = mrb_define_class_under(mrb, mdb_mod, "Txn", mrb->object_class);
  MRB_SET_INSTANCE_TT(mdb_txn_class, MRB_TT_DATA);
  mrb_define_method(mrb, mdb_txn_class, "initialize", mrb_mdb_txn_begin, MRB_ARGS_ARG(1, 2));
  mrb_define_method(mrb, mdb_txn_class, "commit", mrb_mdb_txn_commit, MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_txn_class, "abort", mrb_mdb_txn_abort, MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_txn_class, "reset", mrb_mdb_txn_reset, MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_txn_class, "renew", mrb_mdb_txn_renew, MRB_ARGS_NONE());
  mdb_dbi_mod = mrb_define_module_under(mrb, mdb_mod, "Dbi");
  mrb_define_module_function(mrb, mdb_dbi_mod, "open", mrb_mdb_dbi_open, MRB_ARGS_ARG(1, 2));
  mrb_define_module_function(mrb, mdb_mod, "get",   mrb_mdb_get,  MRB_ARGS_ARG(3, 1));
  mrb_define_module_function(mrb, mdb_mod, "put",   mrb_mdb_put,  MRB_ARGS_ARG(4, 1));
  mrb_define_module_function(mrb, mdb_mod, "drop",  mrb_mdb_drop, MRB_ARGS_ARG(2, 1));
}

void
mrb_mruby_lmdb_gem_final(mrb_state* mrb) {
  /* finalizer */
}

