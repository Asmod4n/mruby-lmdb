const { open } = require('lmdb');
const fs = require('fs');

const N        = 10_000;
const VAL_SIZE = 100;

function nowMs() {
    const [s, ns] = process.hrtime();
    return s * 1000 + ns / 1e6;
}

function fmt(n, ms) {
    return `${ms.toFixed(1).padStart(8)} ms  (${Math.round(n/(ms/1000)).toString().padStart(7)} ops/s)`;
}

async function main() {
    const value = 'x'.repeat(VAL_SIZE);
    const path  = '/tmp/bench_node_lmdb';

    if (fs.existsSync(path)) fs.rmSync(path, { recursive: true });
    fs.mkdirSync(path, { recursive: true });

    const db = open({ path, mapSize: 256*1024*1024, noSync: true, noMetaSync: true, encoding: 'binary' });

    console.log(`=== Node.js LMDB Benchmark (${N} records, ${VAL_SIZE}-byte values) ===\n`);

    // 1. N writes, 1 txn each
    let t0 = nowMs();
    for (let i = 0; i < N; i++)
        await db.transaction(() => { db.put(`key:${String(i).padStart(8,'0')}`, value); });
    let t1 = nowMs();
    console.log(`1. Write (1 txn each):   ${fmt(N, t1-t0)}`);

    // 2. N writes, 1 txn total
    await db.clearAsync();
    t0 = nowMs();
    await db.transaction(() => {
        for (let i = 0; i < N; i++)
            db.put(`key:${String(i).padStart(8,'0')}`, value);
    });
    t1 = nowMs();
    console.log(`2. Write (1 txn total):  ${fmt(N, t1-t0)}`);

    // 3. N reads, 1 txn each
    t0 = nowMs();
    for (let i = 0; i < N; i++)
        db.get(`key:${String(i).padStart(8,'0')}`);
    t1 = nowMs();
    console.log(`3. Read  (1 txn each):   ${fmt(N, t1-t0)}`);

    // 4. N reads, 1 txn total (cursor scan)
    let count = 0;
    t0 = nowMs();
    for (const _ of db.getRange())
        count++;
    t1 = nowMs();
    console.log(`4. Read  (1 txn total):  ${fmt(count, t1-t0)}`);

    // 5. Prefix scan
    await db.transaction(() => {
        for (let i = 0; i < N; i++)
            db.put(`user:${String(i).padStart(6,'0')}`, value);
    });
    count = 0;
    t0 = nowMs();
    for (const { key } of db.getRange({ start: 'user:', end: 'user;' }))
        count++;
    t1 = nowMs();
    console.log(`5. Prefix scan (${count}):   ${fmt(count, t1-t0)}`);

    await db.close();
    fs.rmSync(path, { recursive: true, force: true });
}

main().catch(console.error);