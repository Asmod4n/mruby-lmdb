#pragma once

/*
 * mruby-lmdb C API
 *
 * Basic get/put/del for C callers. All returned strings are copies.
 * For zero-copy access, use the C++ header <mruby/lmdb.hpp> instead.
 */

#include <mruby.h>
#include <lmdb.h>

MRB_BEGIN_DECL

/* Copied mruby String, or mrb_nil_value() if not found. */
MRB_API mrb_value mrb_lmdb_get(mrb_state *mrb, MDB_txn *txn, MDB_dbi dbi,
                                const void *key, size_t key_len);

/* Raises on error. flags: MDB_NOOVERWRITE, MDB_APPEND, etc. */
MRB_API void      mrb_lmdb_put(mrb_state *mrb, MDB_txn *txn, MDB_dbi dbi,
                                const void *key, size_t key_len,
                                const void *val, size_t val_len,
                                unsigned int flags);

/* Returns TRUE if deleted, FALSE if key not found. */
MRB_API mrb_bool  mrb_lmdb_del(mrb_state *mrb, MDB_txn *txn, MDB_dbi dbi,
                                const void *key, size_t key_len);

MRB_END_DECL