# **mruby‑lmdb — Deterministic, Zero‑Magic LMDB Bindings for mruby**

`mruby-lmdb` is a **thin, explicit, zero‑magic binding** to
**Lightning Memory‑Mapped Database (LMDB)** for mruby.

It exposes LMDB exactly as it is:

- **Explicit read/write transactions**
- **Deterministic key ordering**
- **Cursors for full scans and prefix scans**
- **Native‑endian integer key encoding** (`Integer#to_bin` / `String#to_fix`)
- **Crash‑safe, lock‑free reads**
- **No hidden transactions, no buffering layer, no surprises**

If you want an embedded database that is:

- fast
- deterministic
- embeddable
- and safe

…LMDB is the gold standard, and `mruby-lmdb` gives you its real performance model inside mruby.

---

# **Table of Contents**

1. [Quickstart (Fastest APIs)](#quickstart-fastest-apis)
2. [Speed](#speed)
3. [Integer Key Encoding](#integer-key-encoding)
4. [MDB::Env](#mdbenv)
5. [MDB::Database](#mdbdatabase)
6. [MDB::Cursor](#mdbcursor)
7. [MDB::Txn](#mdbtxn)
8. [MDB Module Functions](#mdb-module-functions)
9. [Error Model](#error-model)
10. [Sorting Semantics](#sorting-semantics)
11. [License](#license)

---

# **Quickstart (Fastest APIs)**

These examples use the **actual fastest paths**:

- `db.transaction` for multi‑write and multi‑read
- `.each` for full scans
- `.each_prefix` for prefix scans
- `.multi_get` for bulk reads
- `.batch_put` for bulk writes
- `<<` and `concat` for append‑only integer keys

No magic. No wrappers. No abstractions you don’t have.

---

## **Open an environment and database**

```ruby
env = MDB::Env.new(mapsize: 1 << 30)   # 1 GB virtual map
env.open("/tmp/mydb", MDB::NOSUBDIR)

db = env.database(MDB::INTEGERKEY)
```

---

## **Fastest writes: one explicit write transaction**

```ruby
db.transaction do |txn, dbi|
  MDB.put(txn, dbi, 1.to_bin, "Alice")
  MDB.put(txn, dbi, 2.to_bin, "Bob")
end
```

This is the same pattern used in the benchmark.

---

## **Fastest bulk writes: `batch_put`**

```ruby
db.batch_put([
  [1.to_bin, "Alice"],
  [2.to_bin, "Bob"],
  [3.to_bin, "Carol"],
])
```

Internally: one write transaction.

---

## **Fastest reads: one explicit read transaction**

```ruby
db.transaction(MDB::RDONLY) do |txn, dbi|
  a = MDB.get(txn, dbi, 1.to_bin)
  b = MDB.get(txn, dbi, 2.to_bin)
end
```

---

## **Fastest bulk reads: `multi_get`**

```ruby
values = db.multi_get([1.to_bin, 2.to_bin, 3.to_bin])
# => ["Alice", "Bob", "Carol"]
```

---

## **Fastest full scan: `.each`**

```ruby
db.each do |key, value|
  puts "#{key.to_fix} = #{value}"
end
```

`.each` uses a cursor internally and is the fastest iteration API.

---

## **Fastest prefix scan: `.each_prefix`**

```ruby
db.each_prefix("user:") do |key, value|
  ...
end
```

---

## **Fastest append‑only inserts (INTEGERKEY)**

```ruby
db << "first"
db.concat(["second", "third"])
```

Uses `MDB_APPEND` internally.

---

# **Speed**

`mruby-lmdb` preserves LMDB’s performance model inside mruby.
When you use the fast paths (`db.transaction`, `.each`, `.each_prefix`, `.multi_get`, `.batch_put`), you get **multi‑million ops/sec** throughput.

All numbers below come from the **same benchmark workload** used across C, Python, Node.js, and mruby:

- 10,000 keys
- 100‑byte values
- identical flags (`NOSYNC | NOMETASYNC | NOSUBDIR`)
- identical mapsize (256 MB)
- identical key patterns
- identical prefix‑scan pattern
- identical cursor‑based full scan

### **mruby‑lmdb Performance**

| Operation | Time | Throughput |
|----------|------|------------|
| Write (1 txn each) | **20 ms** | **485,699 ops/s** |
| Write (1 txn total) | **6 ms** | **1,435,279 ops/s** |
| Read (1 txn each) | **5 ms** | **1,677,519 ops/s** |
| Read (1 txn total) | **0 ms** | **10,529,562 ops/s** |
| Prefix scan (10k) | **0 ms** | **10,589,451 ops/s** |

### **Comparison**

| Language | Bulk Write | Bulk Read | Prefix Scan |
|----------|------------|-----------|-------------|
| **C (native)** | 3.9M ops/s | 85M ops/s | 60M ops/s |
| **Python** | 1.5M ops/s | 6.1M ops/s | 3.9M ops/s |
| **Node.js** | 0.9M ops/s | 1.0M ops/s | 1.2M ops/s |
| **mruby‑lmdb** | **1.4M ops/s** | **10.5M ops/s** | **10.6M ops/s** |

### **Interpretation**

- mruby‑lmdb is **faster than Python and Node.js** for bulk reads and prefix scans
- mruby‑lmdb reaches **10+ million ops/sec** in optimal patterns
- even the slow path (one txn per op) is **hundreds of thousands of ops/sec**
- LMDB’s performance characteristics survive intact inside mruby

### **How to hit the fast path**

| Benchmark operation | Fastest mruby‑lmdb API |
|---------------------|------------------------|
| Write (1 txn total) | `db.transaction` or `db.batch_put` |
| Read (1 txn total) | `db.transaction(MDB::RDONLY)` |
| Full scan | `db.each` |
| Prefix scan | `db.each_prefix` |
| Bulk reads | `db.multi_get` |
| Append‑only inserts | `db <<` / `db.concat` |

---

# **Integer Key Encoding**

LMDB’s `MDB_INTEGERKEY` expects **native‑endian, fixed‑width integers**.

`mruby-lmdb` provides:

```ruby
n.to_bin   # => binary string, sizeof(mrb_int) bytes
str.to_fix # => decode back to integer
```

Properties:

- Fixed width
- Native endianness
- Round‑trip safe
- Wrong-sized strings raise `TypeError`

Example:

```ruby
db = env.database(MDB::INTEGERKEY)
db[42.to_bin] = "hello"
db.each { |k, v| puts k.to_fix }
```

---

# **MDB::Env**

```ruby
env = MDB::Env.new(
  mapsize:    10_485_760,
  maxreaders: 200,
  maxdbs:     4
)
env.open("/path", MDB::NOSUBDIR)
```

### Options

- `mapsize:`
- `maxreaders:`
- `maxdbs:`

Invalid keys → `ArgumentError`
Negative values → `RangeError`

### Methods

```ruby
env.stat
env.info
env.path
env.flags
env.maxkeysize
env.reader_check
env.sync(force = false)
env.copy(dest_path, flags = 0)
env.database(flags = 0, name = nil)
env.close
```

---

# **MDB::Database**

```ruby
db = env.database
db = env.database(MDB::CREATE, "named-db")
```

### Basic operations

```ruby
db[key]
db[key] = value
db.del(key)
db.del(key, value)   # for DUPSORT
db.fetch(key, default) { |k| ... }
db.stat
db.length
db.empty?
db.flags
db.drop(delete = false)
```

### Transactions (the fast path)

```ruby
db.transaction(flags = 0) do |txn, dbi|
  MDB.put(txn, dbi, "k", "v")
end
```

- `flags = 0` → write transaction
- `flags = MDB::RDONLY` → read transaction
- commits on success
- aborts and re‑raises on exception

### Bulk helpers

```ruby
db.multi_get(keys)
db.batch_put(pairs)
```

### Iteration

```ruby
db.each { |k, v| ... }
db.each_prefix("user:") { |k, v| ... }
db.each_key("k") { |k, v| ... }   # for DUPSORT
```

### Append‑only (INTEGERKEY)

```ruby
db << "value"
db.concat(["a", "b", "c"])
```

---

# **MDB::Cursor**

```ruby
db.cursor(flags = 0) do |c|
  c.first
  c.next
  c.last
  c.prev
  c.set("key")
  c.set_range("prefix")
  c.put("k", "v", flags = 0)
  c.del(flags = 0)
  c.count
end
```

---

# **MDB::Txn**

```ruby
txn = MDB::Txn.new(env, flags = 0, parent = nil)
```

- requires env
- wrong type → `IOError`
- `commit` is final
- `abort` is idempotent
- `reset`/`renew` work on RDONLY

---

# **MDB Module Functions**

```ruby
MDB.get(txn, dbi, key)
MDB.put(txn, dbi, key, value, flags = 0)
MDB.del(txn, dbi, key, value = nil)
MDB.stat(txn, dbi)
MDB.drop(txn, dbi, delete = false)
MDB.multi_get(txn, dbi, keys)
MDB.batch_put(txn, dbi, pairs)
```

---

# **Error Model**

- `MDB::Error < RuntimeError`
- `MDB::NOTFOUND < MDB::Error`
- `MDB::KEYEXIST < MDB::Error`

---

# **Sorting Semantics**

### String keys

Lexicographic.

### Integer keys (`MDB::INTEGERKEY`)

Numeric.

---

# **License**

Apache 2.0 + OpenLDAP license (LMDB).