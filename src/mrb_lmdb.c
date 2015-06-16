#include "mruby/lmdb.h"
#include "mrb_lmdb.h"

static mrb_value
mrb_fix2bin(mrb_state *mrb, mrb_value self)
{
  mrb_int number = mrb_int(mrb, self);
  mrb_value pp = mrb_str_new(mrb, NULL, sizeof(mrb_int));
  unsigned char *p = (unsigned char *) RSTRING_PTR(pp);

#if PLATFORM_BYTE_ORDER == IS_BIG_ENDIAN
  #if defined(MRB_INT64)
    p[0] = (unsigned char) (number >> 56) & 0xFF;
    p[1] = (unsigned char) (number >> 48) & 0xFF;
    p[2] = (unsigned char) (number >> 40) & 0xFF;
    p[3] = (unsigned char) (number >> 32) & 0xFF;
    p[4] = (unsigned char) (number >> 24) & 0xFF;
    p[5] = (unsigned char) (number >> 16) & 0xFF;
    p[6] = (unsigned char) (number >> 8)  & 0xFF;
    p[7] = (unsigned char) number & 0xFF;
  #elif defined(MRB_INT16)
    p[0] = (unsigned char) (number >> 8)  & 0xFF;
    p[1] = (unsigned char) number & 0xFF;
  #else
    p[0] = (unsigned char) (number >> 24) & 0xFF;
    p[1] = (unsigned char) (number >> 16) & 0xFF;
    p[2] = (unsigned char) (number >> 8)  & 0xFF;
    p[3] = (unsigned char) number & 0xFF;
  #endif
#else
  #if defined(MRB_INT64)
    p[0] = (unsigned char) number & 0xFF;
    p[1] = (unsigned char) (number >> 8)  & 0xFF;
    p[2] = (unsigned char) (number >> 16) & 0xFF;
    p[3] = (unsigned char) (number >> 24) & 0xFF;
    p[4] = (unsigned char) (number >> 32) & 0xFF;
    p[5] = (unsigned char) (number >> 40) & 0xFF;
    p[6] = (unsigned char) (number >> 48) & 0xFF;
    p[7] = (unsigned char) (number >> 56) & 0xFF;
  #elif defined(MRB_INT16)
    p[0] = (unsigned char) number & 0xFF;
    p[1] = (unsigned char) (number >> 8)  & 0xFF;
  #else
    p[0] = (unsigned char) number & 0xFF;
    p[1] = (unsigned char) (number >> 8)  & 0xFF;
    p[2] = (unsigned char) (number >> 16) & 0xFF;
    p[3] = (unsigned char) (number >> 24) & 0xFF;
  #endif
#endif

  return pp;
}

static mrb_value
mrb_bin2fix(mrb_state *mrb, mrb_value self)
{
  if (RSTRING_LEN(self) != sizeof(mrb_int))
    mrb_raise(mrb, E_TYPE_ERROR, "String is not encoded with Fixnum.to_bin");

  unsigned char *p = (unsigned char *) RSTRING_PTR(self);
  mrb_int number;

#if PLATFORM_BYTE_ORDER == IS_BIG_ENDIAN
  #if defined(MRB_INT64)
    number =    (((mrb_int) (p[0])) << 56)
              + (((mrb_int) (p[1])) << 48)
              + (((mrb_int) (p[2])) << 40)
              + (((mrb_int) (p[3])) << 32)
              + (((mrb_int) (p[4])) << 24)
              + (((mrb_int) (p[5])) << 16)
              + (((mrb_int) (p[6])) << 8)
              + (((mrb_int) (p[7]));
  #elif defined(MRB_INT16)
    number =    (((mrb_int) (p[0])) << 8)
              + (((mrb_int) (p[1])));
  #else
    number =    (((mrb_int) (p[0])) << 24)
              + (((mrb_int) (p[1])) << 16)
              + (((mrb_int) (p[2])) << 8)
              + (((mrb_int) (p[3]));
  #endif
#else
  #if defined(MRB_INT64)
    number =    (((mrb_int) (p[0])))
              + (((mrb_int) (p[1])) << 8)
              + (((mrb_int) (p[2])) << 16)
              + (((mrb_int) (p[3])) << 24)
              + (((mrb_int) (p[4])) << 32)
              + (((mrb_int) (p[5])) << 40)
              + (((mrb_int) (p[6])) << 48)
              + (((mrb_int) (p[7])) << 56);
  #elif defined(MRB_INT16)
    number =    (((mrb_int) (p[0])))
              + (((mrb_int) (p[1])) << 8);
  #else
    number =    (((mrb_int) (p[0])))
              + (((mrb_int) (p[1])) << 8)
              + (((mrb_int) (p[2])) << 16)
              + (((mrb_int) (p[3])) << 24);
  #endif
#endif

  return mrb_fixnum_value(number);
}

static void
mrb_mdb_env_free(mrb_state *mrb, void *p)
{
  mdb_env_close((MDB_env *) p);
}

static const struct mrb_data_type mdb_env_type = {
  "$mrb_i_mdb_env", mrb_mdb_env_free,
};

static mrb_value
mrb_mdb_env_create(mrb_state *mrb, mrb_value self)
{
  MDB_env *env;

  int rc = mdb_env_create (&env);

  if (rc != MDB_SUCCESS)
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

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

static mrb_value
mrb_mdb_env_copy(mrb_state *mrb, mrb_value self)
{
  char *path;

  mrb_get_args(mrb, "z", &path);

  int rc = mdb_env_copy((MDB_env *) DATA_PTR(self), (const char *) path);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

static mrb_value
mrb_mdb_env_copy2(mrb_state *mrb, mrb_value self)
{
  char *path;
  mrb_int flags = 0;

  mrb_get_args(mrb, "z|i", &path, &flags);

  if (flags < 0 ||flags > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");

  int rc = mdb_env_copy2((MDB_env *) DATA_PTR(self), (const char *) path, (unsigned int) flags);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

static mrb_value
mrb_mdb_env_stat(mrb_state *mrb, mrb_value self)
{
  MDB_stat stat;
  mrb_value args[6];

  int rc = mdb_env_stat((MDB_env *) DATA_PTR(self), &stat);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  args[0] = stat.ms_psize > MRB_INT_MAX ? mrb_float_value(mrb, stat.ms_psize) : mrb_fixnum_value(stat.ms_psize);
  args[1] = stat.ms_depth > MRB_INT_MAX ? mrb_float_value(mrb, stat.ms_depth) : mrb_fixnum_value(stat.ms_depth);
  args[2] = stat.ms_branch_pages > MRB_INT_MAX ? mrb_float_value(mrb, stat.ms_branch_pages) : mrb_fixnum_value(stat.ms_branch_pages);
  args[3] = stat.ms_leaf_pages > MRB_INT_MAX ? mrb_float_value(mrb, stat.ms_leaf_pages) : mrb_fixnum_value(stat.ms_leaf_pages);
  args[4] = stat.ms_overflow_pages > MRB_INT_MAX ? mrb_float_value(mrb, stat.ms_overflow_pages) : mrb_fixnum_value(stat.ms_overflow_pages);
  args[5] = stat.ms_entries > MRB_INT_MAX ? mrb_float_value(mrb, stat.ms_entries) : mrb_fixnum_value(stat.ms_entries);

  return mrb_obj_new(mrb, LMDB_STAT, 6, args);
}

static mrb_value
mrb_mdb_env_info(mrb_state *mrb, mrb_value self)
{
  MDB_envinfo stat;
  mrb_value args[6];

  int rc = mdb_env_info((MDB_env *) DATA_PTR(self), &stat);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  args[0] = stat.me_mapaddr ? mrb_cptr_value(mrb, stat.me_mapaddr) : mrb_nil_value();
  args[1] = stat.me_mapsize > MRB_INT_MAX ? mrb_float_value(mrb, stat.me_mapsize) : mrb_fixnum_value(stat.me_mapsize);
  args[2] = stat.me_last_pgno > MRB_INT_MAX ? mrb_float_value(mrb, stat.me_last_pgno) : mrb_fixnum_value(stat.me_last_pgno);
  args[3] = stat.me_last_txnid > MRB_INT_MAX ? mrb_float_value(mrb, stat.me_last_txnid) : mrb_fixnum_value(stat.me_last_txnid);
  args[4] = stat.me_maxreaders > MRB_INT_MAX ? mrb_float_value(mrb, stat.me_maxreaders) : mrb_fixnum_value(stat.me_maxreaders);
  args[5] = stat.me_numreaders > MRB_INT_MAX ? mrb_float_value(mrb, stat.me_numreaders) : mrb_fixnum_value(stat.me_numreaders);

  return mrb_obj_new(mrb, LMDB_ENV_INFO, 6, args);
}

static mrb_value
mrb_mdb_env_sync(mrb_state *mrb, mrb_value self)
{
  mrb_bool force = FALSE;

  mrb_get_args(mrb, "|b", &force);

  int rc = mdb_env_sync((MDB_env *) DATA_PTR(self), (int) force);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

static mrb_value
mrb_mdb_env_close(mrb_state* mrb, mrb_value self)
{
  mdb_env_close((MDB_env *) DATA_PTR(self));
  mrb_data_init(self, NULL, &mdb_env_type);

  return mrb_true_value();
}

static mrb_value
mrb_mdb_env_set_flags(mrb_state *mrb, mrb_value self)
{
  mrb_int flags = 0;
  mrb_bool onoff = TRUE;

  mrb_get_args(mrb, "|ib", &flags, &onoff);

  if (flags < 0 ||flags > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");

  int rc = mdb_env_set_flags((MDB_env *) DATA_PTR(self), (unsigned int) flags, (int) onoff);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

static mrb_value
mrb_mdb_env_get_flags(mrb_state *mrb, mrb_value self)
{
  unsigned int flags;

  int rc = mdb_env_get_flags((MDB_env *) DATA_PTR(self), &flags);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  if (flags < MRB_INT_MIN ||flags > MRB_INT_MAX)
    return mrb_float_value(mrb, flags);
  else
    return mrb_fixnum_value(flags);
}

static mrb_value
mrb_mdb_env_get_path(mrb_state *mrb, mrb_value self)
{
  const char *path;

  int rc = mdb_env_get_path((MDB_env *) DATA_PTR(self), &path);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return mrb_str_new_static(mrb, path, strlen(path));
}

static mrb_value
mrb_mdb_env_set_mapsize(mrb_state *mrb, mrb_value self)
{
  mrb_int size;

  mrb_get_args(mrb, "i", &size);

  if (size < 0||size > SIZE_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "size is out of range");

  int rc = mdb_env_set_mapsize((MDB_env *) DATA_PTR(self), (size_t) size);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

static mrb_value
mrb_mdb_env_set_maxreaders(mrb_state *mrb, mrb_value self)
{
  mrb_int readers;

  mrb_get_args(mrb, "i", &readers);

  if (readers < 0 ||readers > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "readers are out of range");

  int rc = mdb_env_set_maxreaders((MDB_env *) DATA_PTR(self), (unsigned int) readers);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

static mrb_value
mrb_mdb_env_get_maxreaders(mrb_state *mrb, mrb_value self)
{
  unsigned int readers;

  int rc = mdb_env_get_maxreaders((MDB_env *) DATA_PTR(self), &readers);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  if (readers > MRB_INT_MAX)
    return mrb_float_value(mrb, readers);
  else
    return mrb_fixnum_value(readers);
}

static mrb_value
mrb_mdb_env_set_maxdbs(mrb_state *mrb, mrb_value self)
{
  mrb_int dbs;

  mrb_get_args(mrb, "i", &dbs);

  if (dbs < 0 ||dbs > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "dbs is out of range");

  int rc = mdb_env_set_maxdbs((MDB_env *) DATA_PTR(self), (MDB_dbi) dbs);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

static mrb_value
mrb_mdb_env_get_maxkeysize(mrb_state *mrb, mrb_value self)
{
  int maxkeysize = mdb_env_get_maxkeysize((MDB_env *) DATA_PTR(self));

  if (maxkeysize > MRB_INT_MAX)
    return mrb_float_value(mrb, maxkeysize);
  else
    return mrb_fixnum_value(maxkeysize);
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
  mrb_int flags = 0;
  MDB_txn *parent = NULL;
  MDB_txn *txn;

  mrb_get_args(mrb, "d|id", &env, &mdb_env_type, &flags, &parent, &mdb_txn_type);

  if (flags < 0 ||flags > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");

  int rc = mdb_txn_begin(env, parent, (unsigned int) flags, &txn);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  mrb_data_init(self, txn, &mdb_txn_type);

  return self;
}

static mrb_value
mrb_mdb_txn_commit(mrb_state *mrb, mrb_value self)
{
  int rc = mdb_txn_commit((MDB_txn *) DATA_PTR(self));
  mrb_data_init(self, NULL, &mdb_txn_type);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return mrb_true_value();
}

static mrb_value
mrb_mdb_txn_abort(mrb_state *mrb, mrb_value self)
{
  mdb_txn_abort((MDB_txn *) DATA_PTR(self));
  mrb_data_init(self, NULL, &mdb_txn_type);

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

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

static mrb_value
mrb_mdb_dbi_open(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn;
  mrb_int flags = 0;
  char *name = NULL;
  MDB_dbi dbi;

  mrb_get_args(mrb, "d|iz", &txn, &mdb_txn_type, &flags, &name);

  if (flags < 0 ||flags > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");

  int rc = mdb_dbi_open(txn, (const char *) name, (unsigned int) flags, &dbi);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  if (dbi > MRB_INT_MAX) {
    mdb_dbi_close(mdb_txn_env(txn), dbi);
    mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");
  }

  return mrb_fixnum_value(dbi);
}

static mrb_value
mrb_mdb_stat(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn;
  mrb_int dbi;
  MDB_stat stat;
  mrb_value args[6];

  mrb_get_args(mrb, "di", &txn, &mdb_txn_type, &dbi);

  if (dbi < 0 ||dbi > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");

  int rc = mdb_stat(txn, (MDB_dbi) dbi, &stat);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  args[0] = stat.ms_psize > MRB_INT_MAX ? mrb_float_value(mrb, stat.ms_psize) : mrb_fixnum_value(stat.ms_psize);
  args[1] = stat.ms_depth > MRB_INT_MAX ? mrb_float_value(mrb, stat.ms_depth) : mrb_fixnum_value(stat.ms_depth);
  args[2] = stat.ms_branch_pages > MRB_INT_MAX ? mrb_float_value(mrb, stat.ms_branch_pages) : mrb_fixnum_value(stat.ms_branch_pages);
  args[3] = stat.ms_leaf_pages > MRB_INT_MAX ? mrb_float_value(mrb, stat.ms_leaf_pages) : mrb_fixnum_value(stat.ms_leaf_pages);
  args[4] = stat.ms_overflow_pages > MRB_INT_MAX ? mrb_float_value(mrb, stat.ms_overflow_pages) : mrb_fixnum_value(stat.ms_overflow_pages);
  args[5] = stat.ms_entries > MRB_INT_MAX ? mrb_float_value(mrb, stat.ms_entries) : mrb_fixnum_value(stat.ms_entries);

  return mrb_obj_new(mrb, LMDB_STAT, 6, args);
}

static mrb_value
mrb_mdb_drop(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn;
  mrb_int dbi;
  mrb_bool del = FALSE;

  mrb_get_args(mrb, "di|b", &txn, &mdb_txn_type, &dbi, &del);

  if (dbi < 0 ||dbi > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");

  int rc = mdb_drop(txn, (MDB_dbi) dbi, (int) del);

  if (rc != MDB_SUCCESS)
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

  if (rc == MDB_SUCCESS) {
    if (static_string)
      return mrb_str_new_static(mrb, (const char *) data.mv_data, data.mv_size);
    else
      return mrb_str_new(mrb, (const char *) data.mv_data, data.mv_size);
  }
  else
  if (rc == MDB_NOTFOUND)
    return mrb_nil_value();
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
  MDB_val key, data;

  mrb_get_args(mrb, "dioo|i", &txn, &mdb_txn_type, &dbi, &key_obj, &data_obj, &flags);

  if (dbi < 0 ||dbi > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");

  if (flags < 0 ||flags > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");

  key_obj = mrb_str_to_str(mrb, key_obj);
  key.mv_size = RSTRING_LEN(key_obj);
  key.mv_data = RSTRING_PTR(key_obj);
  data_obj = mrb_str_to_str(mrb, data_obj);
  data.mv_size = RSTRING_LEN(data_obj);
  data.mv_data = RSTRING_PTR(data_obj);

  int rc = mdb_put(txn, (MDB_dbi) dbi, &key, &data, (unsigned int) flags);

  if(rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

static mrb_value
mrb_mdb_del(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn;
  mrb_int dbi;
  mrb_value key_obj, data_obj;
  MDB_val key, data;

  int args = mrb_get_args(mrb, "dio|o", &txn, &mdb_txn_type, &dbi, &key_obj, &data_obj);

  if (dbi < 0 ||dbi > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");

  key_obj = mrb_str_to_str(mrb, key_obj);
  key.mv_size = RSTRING_LEN(key_obj);
  key.mv_data = RSTRING_PTR(key_obj);

  if (args == 4) {
    data_obj = mrb_str_to_str(mrb, data_obj);
    data.mv_size = RSTRING_LEN(data_obj);
    data.mv_data = RSTRING_PTR(data_obj);
  } else {
    data.mv_size = 0;
    data.mv_data = NULL;
  }

  int rc = mdb_del(txn, (MDB_dbi) dbi, &key, &data);

  if(rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

static void
mrb_mdb_cursor_free(mrb_state *mrb, void *p)
{
  mdb_cursor_close((MDB_cursor *) p);
}

static const struct mrb_data_type mdb_cursor_type = {
  "$mrb_i_mdb_cursor", mrb_mdb_cursor_free,
};

static mrb_value
mrb_mdb_cursor_open(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn;
  mrb_int dbi;
  MDB_cursor *cursor;

  mrb_get_args(mrb, "di", &txn, &mdb_txn_type, &dbi);

  if (dbi < 0 ||dbi > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");

  int rc = mdb_cursor_open(txn, (MDB_dbi) dbi, &cursor);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  mrb_data_init(self, cursor, &mdb_cursor_type);

  return self;
}

static mrb_value
mrb_mdb_cursor_renew(mrb_state *mrb, mrb_value self)
{
  MDB_txn *txn;

  mrb_get_args(mrb, "d", &txn, &mdb_txn_type);

  int rc = mdb_cursor_renew(txn, (MDB_cursor *) DATA_PTR(self));

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

static mrb_value
mrb_mdb_cursor_close(mrb_state *mrb, mrb_value self)
{
  mdb_cursor_close((MDB_cursor *) DATA_PTR(self));
  mrb_data_init(self, NULL, &mdb_cursor_type);

  return mrb_true_value();
}

static mrb_value
mrb_mdb_cursor_get(mrb_state *mrb, mrb_value self)
{
  mrb_int cursor_op;
  mrb_bool static_string = FALSE;
  MDB_val key, data;

  mrb_get_args(mrb, "i|b", &cursor_op, &static_string);

  int rc = mdb_cursor_get((MDB_cursor *) DATA_PTR(self), &key, &data, cursor_op);

  if (rc == MDB_SUCCESS) {
    mrb_value key_data_ary = mrb_ary_new_capa(mrb, 2);
    if (static_string) {
      mrb_ary_set(mrb, key_data_ary, 0,
        mrb_str_new_static(mrb, (const char *) key.mv_data, key.mv_size));
      mrb_ary_set(mrb, key_data_ary, 1,
        mrb_str_new_static(mrb, (const char *) data.mv_data, data.mv_size));
    } else {
      mrb_ary_set(mrb, key_data_ary, 0,
        mrb_str_new(mrb, (const char *) key.mv_data, key.mv_size));
      mrb_ary_set(mrb, key_data_ary, 1,
        mrb_str_new(mrb, (const char *) data.mv_data, data.mv_size));
    }

    return key_data_ary;
  }
  else
  if (rc == MDB_NOTFOUND)
    return mrb_nil_value();
  else
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));
}

static mrb_value
mrb_mdb_cursor_put(mrb_state *mrb, mrb_value self)
{
  mrb_value key_obj, data_obj;
  mrb_int flags = 0;
  MDB_val key, data;

  mrb_get_args(mrb, "oo|i", &key_obj, &data_obj, &flags);

  if (flags < 0 ||flags > UINT_MAX)
    mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");

  key_obj = mrb_str_to_str(mrb, key_obj);
  key.mv_size = RSTRING_LEN(key_obj);
  key.mv_data = RSTRING_PTR(key_obj);
  data_obj = mrb_str_to_str(mrb, data_obj);
  data.mv_size = RSTRING_LEN(data_obj);
  data.mv_data = RSTRING_PTR(data_obj);

  int rc = mdb_cursor_put((MDB_cursor *) DATA_PTR(self), &key, &data, (unsigned int) flags);

  if (rc != MDB_SUCCESS)
    mrb_raise(mrb, E_LMDB_ERROR, mdb_strerror(rc));

  return self;
}

void
mrb_mruby_lmdb_gem_init(mrb_state* mrb) {
  struct RClass *mdb_mod, *mdb_env_class, *mdb_txn_class, *mdb_dbi_mod, *mdb_cursor_class;

  mrb_define_method(mrb, mrb->string_class, "to_fix", mrb_bin2fix, MRB_ARGS_NONE());
  mrb_define_method(mrb, mrb->fixnum_class, "to_bin", mrb_fix2bin, MRB_ARGS_NONE());

  mdb_mod = mrb_define_module(mrb, "MDB");
  mrb_define_const(mrb, mdb_mod,  "VERSION",      mrb_str_new_static(mrb, MDB_VERSION_STRING, strlen(MDB_VERSION_STRING)));
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
  mrb_define_method(mrb, mdb_env_class, "initialize",   mrb_mdb_env_create,         MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_env_class, "open",         mrb_mdb_env_open,           MRB_ARGS_ARG(1, 2));
  mrb_define_method(mrb, mdb_env_class, "copy",         mrb_mdb_env_copy,           MRB_ARGS_REQ(1));
  mrb_define_method(mrb, mdb_env_class, "copy2",        mrb_mdb_env_copy2,          MRB_ARGS_ARG(1, 1));
  mrb_define_method(mrb, mdb_env_class, "stat",         mrb_mdb_env_stat,           MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_env_class, "info",         mrb_mdb_env_info,           MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_env_class, "sync",         mrb_mdb_env_sync,           MRB_ARGS_OPT(1));
  mrb_define_method(mrb, mdb_env_class, "close",        mrb_mdb_env_close,          MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_env_class, "set_flags",    mrb_mdb_env_set_flags,      MRB_ARGS_OPT(2));
  mrb_define_method(mrb, mdb_env_class, "flags",        mrb_mdb_env_get_flags,      MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_env_class, "path",         mrb_mdb_env_get_path,       MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_env_class, "mapsize=",     mrb_mdb_env_set_mapsize,    MRB_ARGS_REQ(1));
  mrb_define_method(mrb, mdb_env_class, "maxreaders=",  mrb_mdb_env_set_maxreaders, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, mdb_env_class, "maxreaders",   mrb_mdb_env_get_maxreaders, MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_env_class, "maxdbs=",      mrb_mdb_env_set_maxdbs,     MRB_ARGS_REQ(1));
  mrb_define_method(mrb, mdb_env_class, "maxkeysize",   mrb_mdb_env_get_maxkeysize, MRB_ARGS_NONE());
  mdb_txn_class = mrb_define_class_under(mrb, mdb_mod, "Txn", mrb->object_class);
  MRB_SET_INSTANCE_TT(mdb_txn_class, MRB_TT_DATA);
  mrb_define_method(mrb, mdb_txn_class, "initialize", mrb_mdb_txn_begin, MRB_ARGS_ARG(1, 2));
  mrb_define_method(mrb, mdb_txn_class, "commit", mrb_mdb_txn_commit, MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_txn_class, "abort", mrb_mdb_txn_abort, MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_txn_class, "reset", mrb_mdb_txn_reset, MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_txn_class, "renew", mrb_mdb_txn_renew, MRB_ARGS_NONE());
  mdb_dbi_mod = mrb_define_module_under(mrb, mdb_mod, "Dbi");
  mrb_define_module_function(mrb, mdb_dbi_mod, "open", mrb_mdb_dbi_open, MRB_ARGS_ARG(1, 2));
  mrb_define_module_function(mrb, mdb_mod, "stat",  mrb_mdb_stat, MRB_ARGS_REQ(2));
  mrb_define_module_function(mrb, mdb_mod, "get",   mrb_mdb_get,  MRB_ARGS_ARG(3, 1));
  mrb_define_module_function(mrb, mdb_mod, "put",   mrb_mdb_put,  MRB_ARGS_ARG(4, 1));
  mrb_define_module_function(mrb, mdb_mod, "del",   mrb_mdb_del,  MRB_ARGS_ARG(3, 1));
  mrb_define_module_function(mrb, mdb_mod, "drop",  mrb_mdb_drop, MRB_ARGS_ARG(2, 1));
  mdb_cursor_class = mrb_define_class_under(mrb, mdb_mod, "Cursor", mrb->object_class);
  MRB_SET_INSTANCE_TT(mdb_cursor_class, MRB_TT_DATA);
  enum MDB_cursor_op cursor_op;
  cursor_op = MDB_FIRST;
  mrb_define_const(mrb, mdb_cursor_class, "FIRST",            mrb_fixnum_value(cursor_op));
  cursor_op = MDB_FIRST_DUP;
  mrb_define_const(mrb, mdb_cursor_class, "FIRST_DUP",        mrb_fixnum_value(cursor_op));
  cursor_op = MDB_GET_BOTH;
  mrb_define_const(mrb, mdb_cursor_class, "GET_BOTH",         mrb_fixnum_value(cursor_op));
  cursor_op = MDB_GET_BOTH_RANGE;
  mrb_define_const(mrb, mdb_cursor_class, "GET_BOTH_RANGE",   mrb_fixnum_value(cursor_op));
  cursor_op = MDB_GET_CURRENT;
  mrb_define_const(mrb, mdb_cursor_class, "GET_CURRENT",      mrb_fixnum_value(cursor_op));
  cursor_op = MDB_GET_MULTIPLE;
  mrb_define_const(mrb, mdb_cursor_class, "GET_MULTIPLE",     mrb_fixnum_value(cursor_op));
  cursor_op = MDB_LAST;
  mrb_define_const(mrb, mdb_cursor_class, "LAST",             mrb_fixnum_value(cursor_op));
  cursor_op = MDB_LAST_DUP;
  mrb_define_const(mrb, mdb_cursor_class, "LAST_DUP",         mrb_fixnum_value(cursor_op));
  cursor_op = MDB_NEXT;
  mrb_define_const(mrb, mdb_cursor_class, "NEXT",             mrb_fixnum_value(cursor_op));
  cursor_op = MDB_NEXT_DUP;
  mrb_define_const(mrb, mdb_cursor_class, "NEXT_DUP",         mrb_fixnum_value(cursor_op));
  cursor_op = MDB_NEXT_MULTIPLE;
  mrb_define_const(mrb, mdb_cursor_class, "NEXT_MULTIPLE",    mrb_fixnum_value(cursor_op));
  cursor_op = MDB_NEXT_NODUP;
  mrb_define_const(mrb, mdb_cursor_class, "NEXT_NODUP",       mrb_fixnum_value(cursor_op));
  cursor_op = MDB_PREV;
  mrb_define_const(mrb, mdb_cursor_class, "PREV",             mrb_fixnum_value(cursor_op));
  cursor_op = MDB_PREV_DUP;
  mrb_define_const(mrb, mdb_cursor_class, "PREV_DUP",         mrb_fixnum_value(cursor_op));
  cursor_op = MDB_PREV_NODUP;
  mrb_define_const(mrb, mdb_cursor_class, "PREV_NODUP",       mrb_fixnum_value(cursor_op));
  cursor_op = MDB_SET;
  mrb_define_const(mrb, mdb_cursor_class, "SET",              mrb_fixnum_value(cursor_op));
  cursor_op = MDB_SET_KEY;
  mrb_define_const(mrb, mdb_cursor_class, "SET_KEY",          mrb_fixnum_value(cursor_op));
  cursor_op = MDB_SET_RANGE;
  mrb_define_const(mrb, mdb_cursor_class, "SET_RANGE",        mrb_fixnum_value(cursor_op));
  mrb_define_method(mrb, mdb_cursor_class, "initialize",  mrb_mdb_cursor_open,  MRB_ARGS_REQ(2));
  mrb_define_method(mrb, mdb_cursor_class, "renew",       mrb_mdb_cursor_renew, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, mdb_cursor_class, "close",       mrb_mdb_cursor_close, MRB_ARGS_NONE());
  mrb_define_method(mrb, mdb_cursor_class, "get",         mrb_mdb_cursor_get,   MRB_ARGS_ARG(1, 1));
  mrb_define_method(mrb, mdb_cursor_class, "put",         mrb_mdb_cursor_put,   MRB_ARGS_ARG(2, 1));
}

void
mrb_mruby_lmdb_gem_final(mrb_state* mrb) {
  /* finalizer */
}

