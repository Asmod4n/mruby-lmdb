#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <lmdb.h>

#define N        10000
#define VAL_SIZE 100
#define MAP_SIZE (256UL * 1024 * 1024)

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1.0e6;
}

static void check(int rc, const char *msg) {
    if (rc != 0) { fprintf(stderr, "%s: %s\n", msg, mdb_strerror(rc)); exit(1); }
}

int main(void) {
    MDB_env *env; MDB_dbi dbi; MDB_txn *txn; MDB_cursor *cursor;
    MDB_val key, data;
    char keybuf[32], valbuf[VAL_SIZE];
    double t0, t1;
    int rc;

    memset(valbuf, 'x', VAL_SIZE);

    const char *path = "/tmp/bench_c_lmdb";
    mkdir(path, 0755);
    check(mdb_env_create(&env), "env_create");
    check(mdb_env_set_mapsize(env, MAP_SIZE), "set_mapsize");
    check(mdb_env_open(env, path, MDB_NOSYNC | MDB_NOMETASYNC, 0644), "env_open");
    check(mdb_txn_begin(env, NULL, 0, &txn), "txn_begin");
    check(mdb_dbi_open(txn, NULL, 0, &dbi), "dbi_open");
    check(mdb_txn_commit(txn), "txn_commit");

    printf("=== C LMDB Benchmark (%d records, %d-byte values) ===\n\n", N, VAL_SIZE);

    /* 1. N writes, 1 txn each */
    t0 = now_ms();
    for (int i = 0; i < N; i++) {
        int len = snprintf(keybuf, sizeof(keybuf), "key:%08d", i);
        key.mv_data = keybuf; key.mv_size = len;
        data.mv_data = valbuf; data.mv_size = VAL_SIZE;
        check(mdb_txn_begin(env, NULL, 0, &txn), "txn_begin");
        check(mdb_put(txn, dbi, &key, &data, 0), "put");
        check(mdb_txn_commit(txn), "txn_commit");
    }
    t1 = now_ms();
    printf("1. Write (1 txn each):   %8.1f ms  (%7.0f ops/s)\n",
           t1-t0, N/((t1-t0)/1000.0));

    /* 2. N writes, 1 txn total */
    check(mdb_txn_begin(env, NULL, 0, &txn), "txn_begin");
    check(mdb_drop(txn, dbi, 0), "drop");
    check(mdb_txn_commit(txn), "txn_commit");

    t0 = now_ms();
    check(mdb_txn_begin(env, NULL, 0, &txn), "txn_begin");
    for (int i = 0; i < N; i++) {
        int len = snprintf(keybuf, sizeof(keybuf), "key:%08d", i);
        key.mv_data = keybuf; key.mv_size = len;
        data.mv_data = valbuf; data.mv_size = VAL_SIZE;
        check(mdb_put(txn, dbi, &key, &data, 0), "put");
    }
    check(mdb_txn_commit(txn), "txn_commit");
    t1 = now_ms();
    printf("2. Write (1 txn total):  %8.1f ms  (%7.0f ops/s)\n",
           t1-t0, N/((t1-t0)/1000.0));

    /* 3. N reads, 1 txn each */
    t0 = now_ms();
    for (int i = 0; i < N; i++) {
        int len = snprintf(keybuf, sizeof(keybuf), "key:%08d", i);
        key.mv_data = keybuf; key.mv_size = len;
        check(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "txn_begin");
        mdb_get(txn, dbi, &key, &data);
        mdb_txn_abort(txn);
    }
    t1 = now_ms();
    printf("3. Read  (1 txn each):   %8.1f ms  (%7.0f ops/s)\n",
           t1-t0, N/((t1-t0)/1000.0));

    /* 4. N reads, 1 txn total (cursor scan) */
    int count = 0;
    t0 = now_ms();
    check(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "txn_begin");
    check(mdb_cursor_open(txn, dbi, &cursor), "cursor_open");
    while (mdb_cursor_get(cursor, &key, &data, count == 0 ? MDB_FIRST : MDB_NEXT) == 0)
        count++;
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
    t1 = now_ms();
    printf("4. Read  (1 txn total):  %8.1f ms  (%7.0f ops/s)\n",
           t1-t0, count/((t1-t0)/1000.0));

    /* 5. Prefix scan */
    check(mdb_txn_begin(env, NULL, 0, &txn), "txn_begin");
    for (int i = 0; i < N; i++) {
        int len = snprintf(keybuf, sizeof(keybuf), "user:%06d", i);
        key.mv_data = keybuf; key.mv_size = len;
        data.mv_data = valbuf; data.mv_size = VAL_SIZE;
        mdb_put(txn, dbi, &key, &data, 0);
    }
    check(mdb_txn_commit(txn), "txn_commit");

    const char *prefix = "user:";
    size_t plen = strlen(prefix);
    count = 0;
    t0 = now_ms();
    check(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "txn_begin");
    check(mdb_cursor_open(txn, dbi, &cursor), "cursor_open");
    key.mv_data = (void*)prefix; key.mv_size = plen;
    rc = mdb_cursor_get(cursor, &key, &data, MDB_SET_RANGE);
    while (rc == 0) {
        if (key.mv_size < plen || memcmp(key.mv_data, prefix, plen) != 0) break;
        count++;
        rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT);
    }
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
    t1 = now_ms();
    printf("5. Prefix scan (%d):    %8.1f ms  (%7.0f ops/s)\n",
           count, t1-t0, count/((t1-t0)/1000.0));

    mdb_env_close(env);
    system("rm -rf /tmp/bench_c_lmdb");
    return 0;
}