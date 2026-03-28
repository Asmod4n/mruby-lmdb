import os, time, shutil, lmdb

N        = 10_000
VAL_SIZE = 100
MAP_SIZE = 256 * 1024 * 1024

def now_ms():
    return time.monotonic() * 1000.0

def fmt(n, ms):
    return f"{ms:8.1f} ms  ({n/(ms/1000):7.0f} ops/s)"

path  = "/tmp/bench_python_lmdb"
value = b'x' * VAL_SIZE

if os.path.exists(path): shutil.rmtree(path)
os.makedirs(path)
env = lmdb.open(path, map_size=MAP_SIZE, sync=False, metasync=False)

print(f"=== Python LMDB Benchmark ({N} records, {VAL_SIZE}-byte values) ===\n")

# 1. N writes, 1 txn each
t0 = now_ms()
for i in range(N):
    with env.begin(write=True) as txn:
        txn.put(f"key:{i:08d}".encode(), value)
t1 = now_ms()
print(f"1. Write (1 txn each):   {fmt(N, t1-t0)}")

# 2. N writes, 1 txn total
with env.begin(write=True) as txn:
    txn.drop(env.open_db(), delete=False)
t0 = now_ms()
with env.begin(write=True) as txn:
    for i in range(N):
        txn.put(f"key:{i:08d}".encode(), value)
t1 = now_ms()
print(f"2. Write (1 txn total):  {fmt(N, t1-t0)}")

# 3. N reads, 1 txn each
t0 = now_ms()
for i in range(N):
    with env.begin() as txn:
        txn.get(f"key:{i:08d}".encode())
t1 = now_ms()
print(f"3. Read  (1 txn each):   {fmt(N, t1-t0)}")

# 4. N reads, 1 txn total (cursor scan)
count = 0
t0 = now_ms()
with env.begin() as txn:
    cursor = txn.cursor()
    for _ in cursor.iternext():
        count += 1
t1 = now_ms()
print(f"4. Read  (1 txn total):  {fmt(count, t1-t0)}")

# 5. Prefix scan
with env.begin(write=True) as txn:
    for i in range(N):
        txn.put(f"user:{i:06d}".encode(), value)
prefix = b"user:"
count = 0
t0 = now_ms()
with env.begin() as txn:
    cursor = txn.cursor()
    cursor.set_range(prefix)
    for k, _ in cursor:
        if not k.startswith(prefix): break
        count += 1
t1 = now_ms()
print(f"5. Prefix scan ({count}):   {fmt(count, t1-t0)}")

env.close()
shutil.rmtree(path, ignore_errors=True)