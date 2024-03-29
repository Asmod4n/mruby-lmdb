#include "mruby/lmdb.h"
#include "mrb_lmdb.h"

static mrb_value
mrb_fix2bin_le(mrb_state* mrb, mrb_value self)
{
    mrb_int number = mrb_integer(self);
    mrb_value pp = mrb_str_new(mrb, NULL, sizeof(mrb_int));
    unsigned char* p = (unsigned char*)RSTRING_PTR(pp);

#ifdef MRB_INT64
    p[0] = (unsigned char)number & 0xFF;
    p[1] = (unsigned char)(number >> 8) & 0xFF;
    p[2] = (unsigned char)(number >> 16) & 0xFF;
    p[3] = (unsigned char)(number >> 24) & 0xFF;
    p[4] = (unsigned char)(number >> 32) & 0xFF;
    p[5] = (unsigned char)(number >> 40) & 0xFF;
    p[6] = (unsigned char)(number >> 48) & 0xFF;
    p[7] = (unsigned char)(number >> 56) & 0xFF;
#elif defined(MRB_INT16)
    p[0] = (unsigned char)number & 0xFF;
    p[1] = (unsigned char)(number >> 8) & 0xFF;
#else
    p[0] = (unsigned char)number & 0xFF;
    p[1] = (unsigned char)(number >> 8) & 0xFF;
    p[2] = (unsigned char)(number >> 16) & 0xFF;
    p[3] = (unsigned char)(number >> 24) & 0xFF;
#endif
    return pp;
}

static mrb_value
mrb_fix2bin_be(mrb_state* mrb, mrb_value self)
{
    mrb_int number = mrb_integer(self);
    mrb_value pp = mrb_str_new(mrb, NULL, sizeof(mrb_int));
    unsigned char* p = (unsigned char*)RSTRING_PTR(pp);

#ifdef MRB_INT64
    p[0] = (unsigned char)(number >> 56) & 0xFF;
    p[1] = (unsigned char)(number >> 48) & 0xFF;
    p[2] = (unsigned char)(number >> 40) & 0xFF;
    p[3] = (unsigned char)(number >> 32) & 0xFF;
    p[4] = (unsigned char)(number >> 24) & 0xFF;
    p[5] = (unsigned char)(number >> 16) & 0xFF;
    p[6] = (unsigned char)(number >> 8) & 0xFF;
    p[7] = (unsigned char)number & 0xFF;
#elif defined(MRB_INT16)
    p[0] = (unsigned char)(number >> 8) & 0xFF;
    p[1] = (unsigned char)number & 0xFF;
#else
    p[0] = (unsigned char)(number >> 24) & 0xFF;
    p[1] = (unsigned char)(number >> 16) & 0xFF;
    p[2] = (unsigned char)(number >> 8) & 0xFF;
    p[3] = (unsigned char)number & 0xFF;
#endif
    return pp;
}

static mrb_value
mrb_bin2fix_be(mrb_state* mrb, mrb_value self)
{
    if (RSTRING_LEN(self) != sizeof(mrb_int)) {
        mrb_raise(mrb, E_TYPE_ERROR, "String is not encoded with Integer.to_bin");
    }

    unsigned char* p = (unsigned char*)RSTRING_PTR(self);
    mrb_int number;

#ifdef MRB_INT64
    number =    (((mrb_int) (p[0])) << 56)
              + (((mrb_int) (p[1])) << 48)
              + (((mrb_int) (p[2])) << 40)
              + (((mrb_int) (p[3])) << 32)
              + (((mrb_int) (p[4])) << 24)
              + (((mrb_int) (p[5])) << 16)
              + (((mrb_int) (p[6])) << 8)
              + (((mrb_int) (p[7])));
#elif defined(MRB_INT16)
        number = (((mrb_int)(p[0])) << 8)
            + (((mrb_int)(p[1])));
#else
    number =    (((mrb_int) (p[0])) << 24)
              + (((mrb_int) (p[1])) << 16)
              + (((mrb_int) (p[2])) << 8)
              + (((mrb_int) (p[3])));
#endif

    return mrb_int_value(mrb, number);
}

static mrb_value
mrb_bin2fix_le(mrb_state* mrb, mrb_value self)
{
    if (RSTRING_LEN(self) != sizeof(mrb_int)) {
        mrb_raise(mrb, E_TYPE_ERROR, "String is not encoded with Integer.to_bin");
    }

    unsigned char* p = (unsigned char*)RSTRING_PTR(self);
    mrb_int number;

#ifdef MRB_INT64
        number = (((mrb_int)(p[0])))
            + (((mrb_int)(p[1])) << 8)
            + (((mrb_int)(p[2])) << 16)
            + (((mrb_int)(p[3])) << 24)
            + (((mrb_int)(p[4])) << 32)
            + (((mrb_int)(p[5])) << 40)
            + (((mrb_int)(p[6])) << 48)
            + (((mrb_int)(p[7])) << 56);
#elif defined(MRB_INT16)
        number = (((mrb_int)(p[0])))
            + (((mrb_int)(p[1])) << 8);
#else
        number = (((mrb_int)(p[0])))
            + (((mrb_int)(p[1])) << 8)
            + (((mrb_int)(p[2])) << 16)
            + (((mrb_int)(p[3])) << 24);
#endif

    return mrb_int_value(mrb, number);
}

static void
mrb_mdb_check_error(mrb_state* mrb, const char* func)
{
    if (errno != MDB_SUCCESS) {
        if (errno >= 0) {
            mrb_sys_fail(mrb, func);
        } else {
            mrb_value error2class = mrb_const_get(mrb, mrb_obj_value(E_LMDB_ERROR), mrb_intern_lit(mrb, "Error2Class"));
            struct RClass* errclass = mrb_class_ptr(mrb_hash_get(mrb, error2class, mrb_int_value(mrb, errno)));
            mrb_value func_str = mrb_str_new_static(mrb, func, strlen(func));
            const char* errstr = mdb_strerror(errno);
            mrb_value error_str = mrb_str_new_static(mrb, errstr, strlen(errstr));
            mrb_raisef(mrb, errclass, "%S: %S", func_str, error_str);
        }
    }
}

static mrb_value
mrb_mdb_env_create(mrb_state* mrb, mrb_value self)
{
    MDB_env* env;

    errno = mdb_env_create(&env);

    mrb_mdb_check_error(mrb, "mdb_env_create");

    mrb_data_init(self, env, &mdb_env_type);

    return self;
}

static mrb_value
mrb_mdb_env_open(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    char* path;
    mrb_int flags = 0, mode = 0600;

    mrb_get_args(mrb, "z|ii", &path, &flags, &mode);

    if (flags < 0||flags > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");
    }

    if (mode < INT_MIN||mode > INT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "mode is out of range");
    }

    errno = mdb_env_open(env, (const char*)path,
        (unsigned int)flags, (mdb_mode_t)mode);

    mrb_mdb_check_error(mrb, "mdb_env_open");

    return self;
}

static mrb_value
mrb_mdb_env_copy(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    char* path;

    mrb_get_args(mrb, "z", &path);

    errno = mdb_env_copy(env, (const char*)path);

    mrb_mdb_check_error(mrb, "mdb_env_copy");

    return self;
}

static mrb_value
mrb_mdb_env_copy2(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    char* path;
    mrb_int flags = 0;

    mrb_get_args(mrb, "z|i", &path, &flags);

    if (flags < 0||flags > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");
    }

    errno = mdb_env_copy2(env, (const char*)path, (unsigned int)flags);

    mrb_mdb_check_error(mrb, "mdb_env_copy2");

    return self;
}

static mrb_value
mrb_mdb_env_stat(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    MDB_stat stat;
    mrb_value args[6];

    errno = mdb_env_stat(env, &stat);

    mrb_mdb_check_error(mrb, "mdb_env_stat");

    args[0] = mrb_int_value(mrb, stat.ms_psize);
    args[1] = mrb_int_value(mrb, stat.ms_depth);
    args[2] = mrb_int_value(mrb, stat.ms_branch_pages);
    args[3] = mrb_int_value(mrb, stat.ms_leaf_pages);
    args[4] = mrb_int_value(mrb, stat.ms_overflow_pages);
    args[5] = mrb_int_value(mrb, stat.ms_entries);

    return mrb_obj_new(mrb, LMDB_STAT, sizeof(args) / sizeof(args[0]), args);
}

static mrb_value
mrb_mdb_env_info(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    MDB_envinfo stat;
    mrb_value args[6];

    errno = mdb_env_info(env, &stat);

    mrb_mdb_check_error(mrb, "mdb_env_info");

    args[0] = stat.me_mapaddr ? mrb_cptr_value(mrb, stat.me_mapaddr) : mrb_nil_value();
    args[1] = mrb_int_value(mrb, stat.me_mapsize);
    args[2] = mrb_int_value(mrb, stat.me_last_pgno);
    args[3] = mrb_int_value(mrb, stat.me_last_txnid);
    args[4] = mrb_int_value(mrb, stat.me_maxreaders);
    args[5] = mrb_int_value(mrb, stat.me_numreaders);

    return mrb_obj_new(mrb, LMDB_ENV_INFO, sizeof(args) / sizeof(args[0]), args);
}

static mrb_value
mrb_mdb_env_sync(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    mrb_bool force = FALSE;

    mrb_get_args(mrb, "|b", &force);

    errno = mdb_env_sync(env, (int)force);

    mrb_mdb_check_error(mrb, "mdb_env_sync");

    return self;
}

static mrb_value
mrb_mdb_env_close(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    if (env) {
        mdb_env_close(env);
        mrb_data_init(self, NULL, NULL);
        return mrb_true_value();
    }

    return mrb_false_value();
}

static mrb_value
mrb_mdb_env_set_flags(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    mrb_int flags = 0;
    mrb_bool onoff = TRUE;

    mrb_get_args(mrb, "|ib", &flags, &onoff);

    if (flags < 0||flags > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");
    }

    errno = mdb_env_set_flags(env, (unsigned int)flags, (int)onoff);

    mrb_mdb_check_error(mrb, "mdb_env_set_flags");

    return self;
}

static mrb_value
mrb_mdb_env_get_flags(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    unsigned int flags;

    errno = mdb_env_get_flags(env, &flags);

    mrb_mdb_check_error(mrb, "mdb_env_get_flags");

    return mrb_int_value(mrb, flags);
}

static mrb_value
mrb_mdb_env_get_path(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    const char* path;

    errno = mdb_env_get_path(env, &path);

    mrb_mdb_check_error(mrb, "mdb_env_get_path");

    return mrb_str_new_static(mrb, path, strlen(path));
}

static mrb_value
mrb_mdb_env_set_mapsize(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    mrb_float size;

    mrb_get_args(mrb, "f", &size);

    if (size < 0||size > SIZE_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "size is out of range");
    }

    errno = mdb_env_set_mapsize(env, (size_t)size);

    mrb_mdb_check_error(mrb, "mdb_env_set_mapsize");

    return self;
}

static mrb_value
mrb_mdb_env_set_maxreaders(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    mrb_int readers;

    mrb_get_args(mrb, "i", &readers);

    if (readers < 0||readers > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "readers are out of range");
    }

    errno = mdb_env_set_maxreaders(env, (unsigned int)readers);

    mrb_mdb_check_error(mrb, "mdb_env_set_maxreaders");

    return self;
}

static mrb_value
mrb_mdb_env_get_maxreaders(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    unsigned int readers;

    errno = mdb_env_get_maxreaders(env, &readers);

    mrb_mdb_check_error(mrb, "mdb_env_get_maxreaders");

    return mrb_int_value(mrb, readers);
}

static mrb_value
mrb_mdb_env_set_maxdbs(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    mrb_int dbs;

    mrb_get_args(mrb, "i", &dbs);

    if (dbs < 0||dbs > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "dbs is out of range");
    }

    errno = mdb_env_set_maxdbs(env, (MDB_dbi)dbs);

    mrb_mdb_check_error(mrb, "mdb_env_set_maxdbs");

    return self;
}

static mrb_value
mrb_mdb_env_get_maxkeysize(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    return mrb_int_value(mrb, mdb_env_get_maxkeysize(env));
}

static mrb_value
mrb_mdb_reader_check(mrb_state* mrb, mrb_value self)
{
    MDB_env* env = (MDB_env*)DATA_PTR(self);
    mrb_assert(env);

    int dead;

    errno = mdb_reader_check(env, &dead);

    mrb_mdb_check_error(mrb, "mdb_reader_check");

    return mrb_int_value(mrb, dead);
}

static mrb_value
mrb_mdb_txn_begin(mrb_state* mrb, mrb_value self)
{
    MDB_env* env;
    mrb_int flags = 0;
    MDB_txn* parent = NULL;
    MDB_txn* txn;

    mrb_get_args(mrb, "d|id!", &env, &mdb_env_type, &flags, &parent, &mdb_txn_type);

    if (flags < 0||flags > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");
    }

    errno = mdb_txn_begin(env, parent, (unsigned int)flags, &txn);

    mrb_mdb_check_error(mrb, "mdb_txn_begin");

    mrb_data_init(self, txn, &mdb_txn_type);

    return self;
}

static mrb_value
mrb_mdb_txn_commit(mrb_state* mrb, mrb_value self)
{
    MDB_txn* txn = (MDB_txn*)DATA_PTR(self);
    mrb_assert(txn);

    errno = mdb_txn_commit(txn);
    mrb_data_init(self, NULL, NULL);

    mrb_mdb_check_error(mrb, "mdb_txn_commit");

    return mrb_true_value();
}

static mrb_value
mrb_mdb_txn_abort(mrb_state* mrb, mrb_value self)
{
    MDB_txn* txn = (MDB_txn*)DATA_PTR(self);
    if (txn) {
        mdb_txn_abort(txn);
        mrb_data_init(self, NULL, NULL);
        return mrb_true_value();
    }

    return mrb_false_value();
}

static mrb_value
mrb_mdb_txn_reset(mrb_state* mrb, mrb_value self)
{
    MDB_txn* txn = (MDB_txn*)DATA_PTR(self);
    mrb_assert(txn);

    mdb_txn_reset(txn);

    return self;
}

static mrb_value
mrb_mdb_txn_renew(mrb_state* mrb, mrb_value self)
{
    MDB_txn* txn = (MDB_txn*)DATA_PTR(self);
    mrb_assert(txn);

    errno = mdb_txn_renew(txn);

    mrb_mdb_check_error(mrb, "mdb_txn_renew");

    return self;
}

static mrb_value
mrb_mdb_dbi_open(mrb_state* mrb, mrb_value self)
{
    MDB_txn* txn;
    mrb_int flags = 0;
    char* name = NULL;
    MDB_dbi dbi;

    mrb_get_args(mrb, "d|iz!", &txn, &mdb_txn_type, &flags, &name);

    if (flags < 0||flags > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");
    }

    errno = mdb_dbi_open(txn, (const char*)name, (unsigned int)flags, &dbi);

    mrb_mdb_check_error(mrb, "mdb_dbi_open");

    return mrb_int_value(mrb, dbi);
}

static mrb_value
mrb_mdb_dbi_flags(mrb_state* mrb, mrb_value self)
{
    MDB_txn* txn;
    mrb_int dbi;
    unsigned int flags;

    mrb_get_args(mrb, "di", &txn, &mdb_txn_type, &dbi);
    if (dbi < 0||dbi > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");
    }

    errno = mdb_dbi_flags(txn, (MDB_dbi)dbi, &flags);

    mrb_mdb_check_error(mrb, "mdb_dbi_flags");

    return mrb_int_value(mrb, flags);
}

static mrb_value
mrb_mdb_stat(mrb_state* mrb, mrb_value self)
{
    MDB_txn* txn;
    mrb_int dbi;
    MDB_stat stat;
    mrb_value args[6];

    mrb_get_args(mrb, "di", &txn, &mdb_txn_type, &dbi);

    if (dbi < 0||dbi > UINT_MAX)
        mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");

    errno = mdb_stat(txn, (MDB_dbi)dbi, &stat);

    mrb_mdb_check_error(mrb, "mdb_stat");

    args[0] = mrb_int_value(mrb, stat.ms_psize);
    args[1] = mrb_int_value(mrb, stat.ms_depth);
    args[2] = mrb_int_value(mrb, stat.ms_branch_pages);
    args[3] = mrb_int_value(mrb, stat.ms_leaf_pages);
    args[4] = mrb_int_value(mrb, stat.ms_overflow_pages);
    args[5] = mrb_int_value(mrb, stat.ms_entries);

    return mrb_obj_new(mrb, LMDB_STAT, sizeof(args) / sizeof(args[0]), args);
}

static mrb_value
mrb_mdb_drop(mrb_state* mrb, mrb_value self)
{
    MDB_txn* txn;
    mrb_int dbi;
    mrb_bool del = FALSE;

    mrb_get_args(mrb, "di|b", &txn, &mdb_txn_type, &dbi, &del);

    if (dbi < 0||dbi > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");
    }

    errno = mdb_drop(txn, (MDB_dbi)dbi, (int)del);

    mrb_mdb_check_error(mrb, "mdb_drop");

    return self;
}

static mrb_value
mrb_mdb_get(mrb_state* mrb, mrb_value self)
{
    MDB_txn* txn;
    mrb_int dbi;
    mrb_value key_obj;
    mrb_bool static_string = FALSE;
    MDB_val key, data;

    mrb_get_args(mrb, "dio|b", &txn, &mdb_txn_type, &dbi, &key_obj, &static_string);

    if (dbi < 0||dbi > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");
    }

    key_obj = mrb_str_to_str(mrb, key_obj);
    key.mv_size = RSTRING_LEN(key_obj);
    key.mv_data = RSTRING_PTR(key_obj);

    errno = mdb_get(txn, (MDB_dbi)dbi, &key, &data);

    if (errno == MDB_SUCCESS) {
        if (static_string) {
            return mrb_str_new_static(mrb, (const char*)data.mv_data, data.mv_size);
        } else {
            return mrb_str_new(mrb, (const char*)data.mv_data, data.mv_size);
        }
    }
    else if (errno == MDB_NOTFOUND) {
        return mrb_nil_value();
    } else {
        mrb_mdb_check_error(mrb, "mdb_get");
    }

    return self;
}

static mrb_value
mrb_mdb_put(mrb_state* mrb, mrb_value self)
{
    MDB_txn* txn;
    mrb_int dbi;
    mrb_value key_obj, data_obj;
    mrb_int flags = 0;
    MDB_val key, data;

    mrb_get_args(mrb, "dioo|i", &txn, &mdb_txn_type, &dbi, &key_obj, &data_obj, &flags);

    if (dbi < 0||dbi > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");
    }

    if (flags < 0||flags > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");
    }

    key_obj = mrb_str_to_str(mrb, key_obj);
    key.mv_size = RSTRING_LEN(key_obj);
    key.mv_data = RSTRING_PTR(key_obj);
    data_obj = mrb_str_to_str(mrb, data_obj);
    data.mv_size = RSTRING_LEN(data_obj);
    data.mv_data = RSTRING_PTR(data_obj);

    errno = mdb_put(txn, (MDB_dbi)dbi, &key, &data, (unsigned int)flags);

    mrb_mdb_check_error(mrb, "mdb_put");

    return self;
}

static mrb_value
mrb_mdb_del(mrb_state* mrb, mrb_value self)
{
    MDB_txn* txn;
    mrb_int dbi;
    mrb_value key_obj, data_obj = mrb_nil_value();
    MDB_val key, data;

    mrb_get_args(mrb, "dio|o", &txn, &mdb_txn_type, &dbi, &key_obj, &data_obj);

    if (dbi < 0||dbi > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");
    }

    key_obj = mrb_str_to_str(mrb, key_obj);
    key.mv_size = RSTRING_LEN(key_obj);
    key.mv_data = RSTRING_PTR(key_obj);

    if (mrb_test(data_obj)) {
        data_obj = mrb_str_to_str(mrb, data_obj);
        data.mv_size = RSTRING_LEN(data_obj);
        data.mv_data = RSTRING_PTR(data_obj);
    } else {
        data.mv_size = 0;
        data.mv_data = NULL;
    }

    errno = mdb_del(txn, (MDB_dbi)dbi, &key, &data);
    if (errno == MDB_SUCCESS) {
        return mrb_true_value();
    }
    else if (errno == MDB_NOTFOUND) {
        return mrb_nil_value();
    } else {
        mrb_mdb_check_error(mrb, "mdb_del");
    }

    return self;
}

static mrb_value
mrb_mdb_cursor_open(mrb_state* mrb, mrb_value self)
{
    MDB_txn* txn;
    mrb_int dbi;
    MDB_cursor* cursor;

    mrb_get_args(mrb, "di", &txn, &mdb_txn_type, &dbi);

    if (dbi < 0||dbi > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "dbi is out of range");
    }

    errno = mdb_cursor_open(txn, (MDB_dbi)dbi, &cursor);

    mrb_mdb_check_error(mrb, "mdb_cursor_open");

    mrb_data_init(self, cursor, &mdb_cursor_type);

    return self;
}

static mrb_value
mrb_mdb_cursor_renew(mrb_state* mrb, mrb_value self)
{
    MDB_cursor* cursor = (MDB_cursor*)DATA_PTR(self);
    mrb_assert(cursor);

    MDB_txn* txn;

    mrb_get_args(mrb, "d", &txn, &mdb_txn_type);

    errno = mdb_cursor_renew(txn, cursor);

    mrb_mdb_check_error(mrb, "mdb_cursor_renew");

    return self;
}

static mrb_value
mrb_mdb_cursor_close(mrb_state* mrb, mrb_value self)
{
    MDB_cursor* cursor = (MDB_cursor*)DATA_PTR(self);
    if (cursor) {
        mdb_cursor_close(cursor);
        mrb_data_init(self, NULL, NULL);
        return mrb_true_value();
    }

    return mrb_false_value();
}

static mrb_value
mrb_mdb_cursor_get(mrb_state* mrb, mrb_value self)
{
    MDB_cursor* cursor = (MDB_cursor*)DATA_PTR(self);
    mrb_assert(cursor);

    mrb_int cursor_op;
    mrb_value key_obj = mrb_nil_value(), data_obj = mrb_nil_value();
    mrb_bool static_string = FALSE;
    MDB_val key, data;

    mrb_get_args(mrb, "i|oob", &cursor_op, &key_obj, &data_obj, &static_string);

    if (mrb_test(key_obj)) {
        key_obj = mrb_str_to_str(mrb, key_obj);
        key.mv_size = RSTRING_LEN(key_obj);
        key.mv_data = RSTRING_PTR(key_obj);
    } else {
        key.mv_size = 0;
        key.mv_data = NULL;
    }
    if (mrb_test(data_obj)) {
        data_obj = mrb_str_to_str(mrb, data_obj);
        data.mv_size = RSTRING_LEN(data_obj);
        data.mv_data = RSTRING_PTR(data_obj);
    } else {
        data.mv_size = 0;
        data.mv_data = NULL;
    }

    errno = mdb_cursor_get(cursor, &key, &data, cursor_op);

    if (errno == MDB_SUCCESS) {
        if (static_string) {
            key_obj = mrb_str_new_static(mrb, (const char*)key.mv_data, key.mv_size);
            data_obj = mrb_str_new_static(mrb, (const char*)data.mv_data, data.mv_size);
        } else {
            key_obj = mrb_str_new(mrb, (const char*)key.mv_data, key.mv_size);
            data_obj = mrb_str_new(mrb, (const char*)data.mv_data, data.mv_size);
        }
        return mrb_assoc_new(mrb, key_obj, data_obj);
    }
    else if (errno == MDB_NOTFOUND) {
        return mrb_nil_value();
    } else {
        mrb_mdb_check_error(mrb, "mdb_cursor_get");
    }

    return self;
}

static mrb_value
mrb_mdb_cursor_put(mrb_state* mrb, mrb_value self)
{
    MDB_cursor* cursor = (MDB_cursor*)DATA_PTR(self);
    mrb_assert(cursor);

    mrb_value key_obj, data_obj;
    mrb_int flags = 0;
    MDB_val key, data;

    mrb_get_args(mrb, "oo|i", &key_obj, &data_obj, &flags);

    if (flags < 0||flags > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");
    }

    key_obj = mrb_str_to_str(mrb, key_obj);
    key.mv_size = RSTRING_LEN(key_obj);
    key.mv_data = RSTRING_PTR(key_obj);
    data_obj = mrb_str_to_str(mrb, data_obj);
    data.mv_size = RSTRING_LEN(data_obj);
    data.mv_data = RSTRING_PTR(data_obj);

    errno = mdb_cursor_put(cursor, &key, &data, (unsigned int)flags);

    mrb_mdb_check_error(mrb, "mdb_cursor_put");

    return self;
}

static mrb_value
mrb_mdb_cursor_del(mrb_state* mrb, mrb_value self)
{
    MDB_cursor* cursor = (MDB_cursor*)DATA_PTR(self);
    mrb_assert(cursor);

    mrb_int flags = 0;

    mrb_get_args(mrb, "|i", &flags);

    if (flags < 0||flags > UINT_MAX) {
        mrb_raise(mrb, E_RANGE_ERROR, "flags are out of range");
    }

    errno = mdb_cursor_del(cursor, (unsigned int)flags);

    mrb_mdb_check_error(mrb, "mdb_cursor_del");

    return self;
}

static mrb_value
mrb_mdb_cursor_count(mrb_state* mrb, mrb_value self)
{
    MDB_cursor* cursor = (MDB_cursor*)DATA_PTR(self);
    mrb_assert(cursor);

    size_t count;

    errno = mdb_cursor_count(cursor, &count);

    mrb_mdb_check_error(mrb, "mdb_cursor_count");

    return mrb_int_value(mrb, count);
}

void
mrb_mruby_lmdb_gem_init(mrb_state* mrb)
{

    struct RClass *mdb_mod, *mdb_error, *mdb_env_class, *mdb_txn_class, *mdb_dbi_mod, *mdb_cursor_class;

#ifdef MRB_ENDIAN_BIG
    mrb_define_method(mrb, mrb->string_class, "to_fix", mrb_bin2fix_be, MRB_ARGS_NONE());
    mrb_define_method(mrb, mrb->integer_class, "to_bin", mrb_fix2bin_be, MRB_ARGS_NONE());
#else
    mrb_define_method(mrb, mrb->string_class, "to_fix", mrb_bin2fix_le, MRB_ARGS_NONE());
    mrb_define_method(mrb, mrb->integer_class, "to_bin", mrb_fix2bin_le, MRB_ARGS_NONE());
#endif

    mdb_mod = mrb_define_module(mrb, "MDB");
    mrb_define_const(mrb, mdb_mod, "VERSION", mrb_str_new_lit(mrb, MDB_VERSION_STRING));
    mrb_define_const(mrb, mdb_mod, "FIXEDMAP", mrb_int_value(mrb, MDB_FIXEDMAP));
    mrb_define_const(mrb, mdb_mod, "NOSUBDIR", mrb_int_value(mrb, MDB_NOSUBDIR));
    mrb_define_const(mrb, mdb_mod, "NOSYNC", mrb_int_value(mrb, MDB_NOSYNC));
    mrb_define_const(mrb, mdb_mod, "RDONLY", mrb_int_value(mrb, MDB_RDONLY));
    mrb_define_const(mrb, mdb_mod, "NOMETASYNC", mrb_int_value(mrb, MDB_NOMETASYNC));
    mrb_define_const(mrb, mdb_mod, "WRITEMAP", mrb_int_value(mrb, MDB_WRITEMAP));
    mrb_define_const(mrb, mdb_mod, "MAPASYNC", mrb_int_value(mrb, MDB_MAPASYNC));
    mrb_define_const(mrb, mdb_mod, "NOTLS", mrb_int_value(mrb, MDB_NOTLS));
    mrb_define_const(mrb, mdb_mod, "NOLOCK", mrb_int_value(mrb, MDB_NOLOCK));
    mrb_define_const(mrb, mdb_mod, "NORDAHEAD", mrb_int_value(mrb, MDB_NORDAHEAD));
    mrb_define_const(mrb, mdb_mod, "NOMEMINIT", mrb_int_value(mrb, MDB_NOMEMINIT));
    mrb_define_const(mrb, mdb_mod, "REVERSEKEY", mrb_int_value(mrb, MDB_REVERSEKEY));
    mrb_define_const(mrb, mdb_mod, "DUPSORT", mrb_int_value(mrb, MDB_DUPSORT));
    mrb_define_const(mrb, mdb_mod, "INTEGERKEY", mrb_int_value(mrb, MDB_INTEGERKEY));
    mrb_define_const(mrb, mdb_mod, "DUPFIXED", mrb_int_value(mrb, MDB_DUPFIXED));
    mrb_define_const(mrb, mdb_mod, "INTEGERDUP", mrb_int_value(mrb, MDB_INTEGERDUP));
    mrb_define_const(mrb, mdb_mod, "REVERSEDUP", mrb_int_value(mrb, MDB_REVERSEDUP));
    mrb_define_const(mrb, mdb_mod, "CREATE", mrb_int_value(mrb, MDB_CREATE));
    mrb_define_const(mrb, mdb_mod, "NOOVERWRITE", mrb_int_value(mrb, MDB_NOOVERWRITE));
    mrb_define_const(mrb, mdb_mod, "NODUPDATA", mrb_int_value(mrb, MDB_NODUPDATA));
    mrb_define_const(mrb, mdb_mod, "CURRENT", mrb_int_value(mrb, MDB_CURRENT));
    mrb_define_const(mrb, mdb_mod, "RESERVE", mrb_int_value(mrb, MDB_RESERVE));
    mrb_define_const(mrb, mdb_mod, "APPEND", mrb_int_value(mrb, MDB_APPEND));
    mrb_define_const(mrb, mdb_mod, "APPENDDUP", mrb_int_value(mrb, MDB_APPENDDUP));
    mrb_define_const(mrb, mdb_mod, "MULTIPLE", mrb_int_value(mrb, MDB_MULTIPLE));
    mrb_define_const(mrb, mdb_mod, "CP_COMPACT", mrb_int_value(mrb, MDB_CP_COMPACT));

    mdb_error = mrb_define_class_under(mrb, mdb_mod, "Error", E_RUNTIME_ERROR);

    mdb_env_class = mrb_define_class_under(mrb, mdb_mod, "Env", mrb->object_class);
    MRB_SET_INSTANCE_TT(mdb_env_class, MRB_TT_DATA);
    mrb_define_method(mrb, mdb_env_class, "initialize", mrb_mdb_env_create, MRB_ARGS_NONE());
    mrb_define_method(mrb, mdb_env_class, "open", mrb_mdb_env_open, MRB_ARGS_ARG(1, 2));
    mrb_define_method(mrb, mdb_env_class, "copy", mrb_mdb_env_copy, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, mdb_env_class, "copy2", mrb_mdb_env_copy2, MRB_ARGS_ARG(1, 1));
    mrb_define_method(mrb, mdb_env_class, "stat", mrb_mdb_env_stat, MRB_ARGS_NONE());
    mrb_define_method(mrb, mdb_env_class, "info", mrb_mdb_env_info, MRB_ARGS_NONE());
    mrb_define_method(mrb, mdb_env_class, "sync", mrb_mdb_env_sync, MRB_ARGS_OPT(1));
    mrb_define_method(mrb, mdb_env_class, "close", mrb_mdb_env_close, MRB_ARGS_NONE());
    mrb_define_method(mrb, mdb_env_class, "set_flags", mrb_mdb_env_set_flags, MRB_ARGS_OPT(2));
    mrb_define_method(mrb, mdb_env_class, "flags", mrb_mdb_env_get_flags, MRB_ARGS_NONE());
    mrb_define_method(mrb, mdb_env_class, "path", mrb_mdb_env_get_path, MRB_ARGS_NONE());
    mrb_define_method(mrb, mdb_env_class, "mapsize=", mrb_mdb_env_set_mapsize, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, mdb_env_class, "maxreaders=", mrb_mdb_env_set_maxreaders, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, mdb_env_class, "maxreaders", mrb_mdb_env_get_maxreaders, MRB_ARGS_NONE());
    mrb_define_method(mrb, mdb_env_class, "maxdbs=", mrb_mdb_env_set_maxdbs, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, mdb_env_class, "maxkeysize", mrb_mdb_env_get_maxkeysize, MRB_ARGS_NONE());
    mrb_define_method(mrb, mdb_env_class, "reader_check", mrb_mdb_reader_check, MRB_ARGS_NONE());

    mdb_txn_class = mrb_define_class_under(mrb, mdb_mod, "Txn", mrb->object_class);
    MRB_SET_INSTANCE_TT(mdb_txn_class, MRB_TT_DATA);
    mrb_define_method(mrb, mdb_txn_class, "initialize", mrb_mdb_txn_begin, MRB_ARGS_ARG(1, 2));
    mrb_define_method(mrb, mdb_txn_class, "commit", mrb_mdb_txn_commit, MRB_ARGS_NONE());
    mrb_define_method(mrb, mdb_txn_class, "abort", mrb_mdb_txn_abort, MRB_ARGS_NONE());
    mrb_define_method(mrb, mdb_txn_class, "reset", mrb_mdb_txn_reset, MRB_ARGS_NONE());
    mrb_define_method(mrb, mdb_txn_class, "renew", mrb_mdb_txn_renew, MRB_ARGS_NONE());

    mdb_dbi_mod = mrb_define_module_under(mrb, mdb_mod, "Dbi");
    mrb_define_module_function(mrb, mdb_dbi_mod, "open", mrb_mdb_dbi_open, MRB_ARGS_ARG(1, 2));
    mrb_define_module_function(mrb, mdb_dbi_mod, "flags", mrb_mdb_dbi_flags, MRB_ARGS_REQ(2));
    mrb_define_module_function(mrb, mdb_mod, "stat", mrb_mdb_stat, MRB_ARGS_REQ(2));
    mrb_define_module_function(mrb, mdb_mod, "get", mrb_mdb_get, MRB_ARGS_ARG(3, 1));
    mrb_define_module_function(mrb, mdb_mod, "put", mrb_mdb_put, MRB_ARGS_ARG(4, 1));
    mrb_define_module_function(mrb, mdb_mod, "del", mrb_mdb_del, MRB_ARGS_ARG(3, 1));
    mrb_define_module_function(mrb, mdb_mod, "drop", mrb_mdb_drop, MRB_ARGS_ARG(2, 1));

    mdb_cursor_class = mrb_define_class_under(mrb, mdb_mod, "Cursor", mrb->object_class);
    MRB_SET_INSTANCE_TT(mdb_cursor_class, MRB_TT_DATA);
    mrb_define_const(mrb, mdb_cursor_class, "FIRST", mrb_int_value(mrb, MDB_FIRST));
    mrb_define_const(mrb, mdb_cursor_class, "FIRST_DUP", mrb_int_value(mrb, MDB_FIRST_DUP));
    mrb_define_const(mrb, mdb_cursor_class, "GET_BOTH", mrb_int_value(mrb, MDB_GET_BOTH));
    mrb_define_const(mrb, mdb_cursor_class, "GET_BOTH_RANGE", mrb_int_value(mrb, MDB_GET_BOTH_RANGE));
    mrb_define_const(mrb, mdb_cursor_class, "GET_CURRENT", mrb_int_value(mrb, MDB_GET_CURRENT));
    mrb_define_const(mrb, mdb_cursor_class, "GET_MULTIPLE", mrb_int_value(mrb, MDB_GET_MULTIPLE));
    mrb_define_const(mrb, mdb_cursor_class, "LAST", mrb_int_value(mrb, MDB_LAST));
    mrb_define_const(mrb, mdb_cursor_class, "LAST_DUP", mrb_int_value(mrb, MDB_LAST_DUP));
    mrb_define_const(mrb, mdb_cursor_class, "NEXT", mrb_int_value(mrb, MDB_NEXT));
    mrb_define_const(mrb, mdb_cursor_class, "NEXT_DUP", mrb_int_value(mrb, MDB_NEXT_DUP));
    mrb_define_const(mrb, mdb_cursor_class, "NEXT_MULTIPLE", mrb_int_value(mrb, MDB_NEXT_MULTIPLE));
    mrb_define_const(mrb, mdb_cursor_class, "NEXT_NODUP", mrb_int_value(mrb, MDB_NEXT_NODUP));
    mrb_define_const(mrb, mdb_cursor_class, "PREV", mrb_int_value(mrb, MDB_PREV));
    mrb_define_const(mrb, mdb_cursor_class, "PREV_DUP", mrb_int_value(mrb, MDB_PREV_DUP));
    mrb_define_const(mrb, mdb_cursor_class, "PREV_NODUP", mrb_int_value(mrb, MDB_PREV_NODUP));
    mrb_define_const(mrb, mdb_cursor_class, "SET", mrb_int_value(mrb, MDB_SET));
    mrb_define_const(mrb, mdb_cursor_class, "SET_KEY", mrb_int_value(mrb, MDB_SET_KEY));
    mrb_define_const(mrb, mdb_cursor_class, "SET_RANGE", mrb_int_value(mrb, MDB_SET_RANGE));
#ifdef MDB_PREV_MULTIPLE
    mrb_define_const(mrb, mdb_cursor_class, "PREV_MULTIPLE", mrb_int_value(mrb, MDB_PREV_MULTIPLE));
#endif
    mrb_define_method(mrb, mdb_cursor_class, "initialize", mrb_mdb_cursor_open, MRB_ARGS_REQ(2));
    mrb_define_method(mrb, mdb_cursor_class, "renew", mrb_mdb_cursor_renew, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, mdb_cursor_class, "close", mrb_mdb_cursor_close, MRB_ARGS_NONE());
    mrb_define_method(mrb, mdb_cursor_class, "get", mrb_mdb_cursor_get, MRB_ARGS_ARG(1, 3));
    mrb_define_method(mrb, mdb_cursor_class, "put", mrb_mdb_cursor_put, MRB_ARGS_ARG(2, 1));
    mrb_define_method(mrb, mdb_cursor_class, "del", mrb_mdb_cursor_del, MRB_ARGS_OPT(1));
    mrb_define_method(mrb, mdb_cursor_class, "count", mrb_mdb_cursor_count, MRB_ARGS_NONE());

    mrb_value error2class = mrb_hash_new(mrb);
    mrb_define_const(mrb, mdb_error, "Error2Class", error2class);

#define mrb_lmdb_define_error(MDB_ERROR, RB_CLASS_NAME) \
    do { \
        int ai = mrb_gc_arena_save(mrb); \
        struct RClass *err = mrb_define_class_under(mrb, mdb_mod, RB_CLASS_NAME, mdb_error); \
        mrb_hash_set(mrb, error2class, mrb_int_value(mrb, MDB_ERROR), mrb_obj_value(err)); \
        mrb_gc_arena_restore(mrb, ai); \
    } while(0)

#include "known_errors_def.cstub"

    mrb_value cursor_ops = mrb_hash_new(mrb);
    mrb_define_const(mrb, mdb_cursor_class, "Ops", cursor_ops);

#define mrb_lmdb_define_cursor_op(MDB_CURSOR_OP, RB_CURSOR_OP_SYM) \
    do { \
        int ai = mrb_gc_arena_save(mrb); \
        mrb_hash_set(mrb, cursor_ops, mrb_symbol_value(mrb_intern_lit(mrb, RB_CURSOR_OP_SYM)), mrb_int_value(mrb, MDB_CURSOR_OP)); \
        mrb_gc_arena_restore(mrb, ai); \
    } while(0)

#include "known_cursor_ops_def.cstub"
}

void mrb_mruby_lmdb_gem_final(mrb_state* mrb) {}
