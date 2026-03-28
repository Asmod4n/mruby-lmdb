# 📦 Migration Guide for 2.0
### Handling the new big‑endian integer encoding

Version 2.0 introduces a **strict, platform‑independent big‑endian encoding** for integers.
This ensures correct LMDB key ordering, but it also means that **binary integer keys written by older versions cannot be read back correctly**.

This guide explains how to migrate.

---

## 🔍 What changed?

### Before 2.0
- Integer encoding depended on the **machine’s native endianness**.
- A database created on little‑endian hardware produced different key bytes than one created on big‑endian hardware.
- LMDB sorts keys lexicographically, so numeric ordering was not guaranteed.

### Starting with 2.0
- Integers are always encoded in **big‑endian**.
- Encoding width is fixed to **2, 4, or 8 bytes** depending on `MRB_INT_BIT`.
- Output is now **stable, portable, and correctly sortable**.

---

## ⚠️ Will my existing database still work?

It depends:

### You *are affected* if:
- You used `<<` or `concat` to build LMDB keys that included integers.
- You stored integers using the old `Fixnum#to_bin` / `String#to_int`.

### You are *not affected* if:
- You never stored integers as part of LMDB keys.
- You stored integers only inside values (not keys).
- You used your own serialization format.

If you are affected, you must **re‑encode your keys**.

---

## 🔧 How to migrate an existing database

There are two realistic migration strategies:

---

## **Option 1: Rebuild the database (recommended)**

If your database is small or can be regenerated:

1. Open the old DB using the **old version** of the library.
2. Iterate over all key/value pairs.
3. Convert the old integer keys back using the old `bin2fix`.
4. Re‑encode them using the new `fix2bin`.
5. Write everything into a fresh LMDB environment.

This guarantees a clean, correct result.

---

## **Option 2: Write a one‑time migration script**

If you need an in‑place upgrade, you can write a script that:

1. Reads each key as raw bytes.
2. Decodes it using the **old** logic (endianness‑dependent).
3. Re‑encodes it using the **new** big‑endian logic.
4. Writes the new key/value pair into a new DB.

You cannot safely do this inside the same LMDB environment because LMDB does not allow key mutation — you must write into a new DB.

---

## 🧪 How to detect old vs. new keys

If you need to distinguish them programmatically:

- Old keys match the machine’s native endianness.
- New keys always start with the **most significant byte**.

Example for 32‑bit ints:

| Value | Old (little‑endian) | New (big‑endian) |
|-------|----------------------|------------------|
| 1     | `01 00 00 00`        | `00 00 00 01`    |
| 255   | `FF 00 00 00`        | `00 00 00 FF`    |

If you see the least significant byte first, it’s an old key.

---

## 🛡️ Why this change was necessary

LMDB sorts keys lexicographically.
If integers are not encoded in big‑endian, then:

- `2` sorts *after* `1000`
- `10` sorts *before* `2`
- ordering depends on CPU architecture

This breaks range scans, prefix scans, and any logic that relies on numeric ordering.

The new encoding fixes all of this permanently.

---

# mruby-lmdb
mruby wrapper for Lightning Memory-Mapped Database from Symas http://symas.com/mdb/

# mruby-lmdb Usage Guide

## Performance Model

LMDB is a B+tree on memory-mapped pages. Understanding the cost model
prevents surprises at scale.

### Operation Costs

| Operation | Time | Allocations | Notes |
|-----------|------|-------------|-------|
| `db["key"]` | O(log n) | 2: txn + String copy | Opens+aborts a txn every call |
| `db["key"] = val` | O(log n) | 1: txn | Writes are serialized (one writer) |
| `db.snapshot { get; get; }` | O(log n) per get | 1 txn + 1 String per get | **Preferred for multi-read** |
| `db.each { }` | O(n) | 1 txn + 1 cursor + 2 Strings/entry | Full scan, copies every k/v |
| `db.each_prefix("p:") { }` | O(log n + m) | 1 txn + 1 cursor + 2 Strings/match | m = matching entries |
| `db.batch { put; put; }` | O(k log n) | 1 txn | k = number of puts |
| `db.del("key")` | O(log n) | 1 txn | |
| `db.length` | O(1) | 1 txn | Reads metadata page |
| C++ `ReadTxn::get` | O(log n) | **0** (zero-copy) | Pointer into mmap |
| C++ `ReadTxn::each_prefix` | O(log n + m) | **0** per entry | Zero-copy iteration |

### What's Fast

```ruby
# ✅ GOOD: Batch reads in one transaction
# Cost: 1 txn open + N × O(log n) lookups + 1 txn abort
# Total allocations: 1 txn + N strings
db.snapshot do |s|
  user    = s.get("user:1")      # O(log n), 1 memcpy
  config  = s.get("config")      # O(log n), 1 memcpy, same txn
  session = s.get("session:abc") # O(log n), 1 memcpy, same snapshot
end

# ✅ GOOD: Batch writes in one transaction
# Cost: 1 txn open + N × O(log n) inserts + 1 fsync + 1 txn commit
# 100× faster than N individual db["k"] = "v" calls
db.batch do |txn, dbi|
  1000.times { |i| MDB.put(txn, dbi, "key:#{i}", "val:#{i}") }
end

# ✅ GOOD: Prefix scan for related keys
# Cost: O(log n) seek + O(m) iteration, m = matching keys
# Only touches the B-tree pages containing "user:" keys
db.each_prefix("user:") do |key, val|
  # process user records
end

# ✅ GOOD: Cursor for range queries
db.cursor(MDB::RDONLY) do |c|
  record = c.set_range("invoice:2025-01")  # O(log n) seek
  while record
    key = record[0]
    break unless key.start_with?("invoice:2025-")
    # process January 2025 invoices
    record = c.next  # O(1) — sequential leaf page access
  end
end
```

### What's Slow (Anti-Patterns)

```ruby
# ❌ BAD: Individual reads in a loop
# Cost: N × (txn_begin + O(log n) + memcpy + txn_abort)
# Each db["key"] opens and closes a full read transaction
keys.each do |k|
  val = db[k]       # 1 txn per read!
  process(val)
end

# ✅ FIX: Use snapshot
db.snapshot do |s|
  keys.each do |k|
    val = s.get(k)  # same txn, N × O(log n) only
    process(val)
  end
end


# ❌ BAD: Individual writes in a loop
# Cost: N × (txn_begin + O(log n) + fsync + txn_commit)
# Each write does a disk fsync!
1000.times { |i| db["key:#{i}"] = "val:#{i}" }  # 1000 fsyncs

# ✅ FIX: Use batch (1 fsync)
db.batch do |txn, dbi|
  1000.times { |i| MDB.put(txn, dbi, "key:#{i}", "val:#{i}") }
end


# ❌ BAD: Full scan to find one key
# Cost: O(n) — touches every page in the database
result = nil
db.each do |k, v|
  if k == "target"
    result = v
    break
  end
end

# ✅ FIX: Direct lookup
result = db["target"]  # O(log n)


# ❌ BAD: Checking existence by catching nil
val = db["maybe"]
if val
  db["maybe"] = transform(val)  # 2 separate transactions!
end

# ✅ FIX: Read-modify-write in one transaction
db.batch do |txn, dbi|
  val = MDB.get(txn, dbi, "maybe")
  if val
    MDB.put(txn, dbi, "maybe", transform(val))
  end
end


# ❌ BAD: Using each_prefix for exact match
db.each_prefix("exact_key") do |k, v|  # scans all keys starting with "exact_key"
  break if k == "exact_key"
end

# ✅ FIX: Direct get
val = db["exact_key"]  # O(log n), done
```


## C++ Zero-Copy API

The C++ header provides zero-copy access for performance-critical paths.
All pointers borrow from the mmap — no allocations, no copies.

**Runtime safety:** Slices carry a validity flag shared with the ReadTxn.
Accessing a Slice after the ReadTxn is destroyed raises `RuntimeError`.

```cpp
#include <mruby/lmdb.hpp>

// ── Basic: batch reads, zero copy ────────────────────────────
{
  LMDB::ReadTxn rtx(mrb, env, dbi);

  auto user = rtx.get("user:1");
  if (user) {
    // user.data() → const uint8_t* into mmap page
    // user.len()  → size_t
    // Zero allocations. O(log n).
    process(user.data(), user.len());
  }

  // Multiple gets: same txn, consistent snapshot
  auto a = rtx.get("key_a");
  auto b = rtx.get("key_b");

  // Need data after rtx dies? Copy BEFORE destruction:
  mrb_value owned;
  if (a) owned = a.to_str(mrb);  // memcpy into mruby String

} // ~ReadTxn: txn aborted, all Slices invalidated
  // a.data() here would raise RuntimeError


// ── Prefix scan: zero alloc per entry ────────────────────────
{
  LMDB::ReadTxn rtx(mrb, env, dbi);

  rtx.each_prefix("user:", [&](LMDB::Slice key, LMDB::Slice val) {
    // key.data(), val.data() are mmap pointers
    // valid inside this lambda only
    // O(1) per entry after initial O(log n) seek
    return true;  // continue
  });
}



// ── Runtime safety demo ──────────────────────────────────────
LMDB::Slice leaked;
{
  LMDB::ReadTxn rtx(mrb, env, dbi);
  leaked = rtx.get("key");    // compiles — Slice is copyable
} // ~ReadTxn invalidates the guard

leaked.data();  // RAISES RuntimeError: "Slice used after ReadTxn was closed"
leaked.len();   // RAISES RuntimeError
leaked.valid(); // returns false (safe to call, no exception)


// ── Anti-pattern: do NOT do this ─────────────────────────────
// Opening a ReadTxn and never closing it pins the LMDB snapshot.
// Old pages can't be reclaimed. The database file grows without bound.
// ALWAYS use scoped ReadTxn (RAII guarantees cleanup).
```

## Memory & LMDB Overhead

| Resource | Cost | When released |
|----------|------|---------------|
| `MDB::Env` | mmap of entire DB file | `env.close` |
| Read txn (Ruby `snapshot {}`) | 1 reader slot, pins snapshot | block exit |
| Write txn (Ruby `batch {}`) | 1 writer lock (exclusive) | block exit |
| C++ `ReadTxn` | 1 reader slot + 16 bytes (Guard) | destructor |
| C++ `Slice` | 0 bytes heap (borrows pointer) | destructor (releases Guard ref) |
| Ruby `db["key"]` | 1 reader slot + 1 String copy | immediate |
| Ruby `db["key"] = val` | 1 writer lock + 1 fsync | immediate |

### Reader slot exhaustion

LMDB has a fixed number of reader slots (default 126). Each open
read transaction consumes one. If you exhaust them, new reads fail
with `MDB_READERS_FULL`.

Causes:
- Long-lived `snapshot {}` blocks
- Leaked `MDB::Txn` objects (forgot to abort)
- C++ `ReadTxn` held too long

Fix: keep read scopes short. Call `env.reader_check` periodically
to reclaim slots from dead processes.

### Map size

LMDB pre-allocates a virtual address range (mapsize). The actual file
grows as needed but can never exceed mapsize. Set it generously:

```ruby
env = MDB::Env.new(mapsize: 1073741824)  # 1 GB virtual, file starts small
```

Increasing mapsize requires closing and reopening the env.

License
=======
Copyright 2026

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this software except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Acknowledgements
================
This is using code from https://github.com/LMDB/lmdb

The OpenLDAP Public License
  Version 2.8, 17 August 2003

Redistribution and use of this software and associated documentation
("Software"), with or without modification, are permitted provided
that the following conditions are met:

1. Redistributions in source form must retain copyright statements
   and notices,

2. Redistributions in binary form must reproduce applicable copyright
   statements and notices, this list of conditions, and the following
   disclaimer in the documentation and/or other materials provided
   with the distribution, and

3. Redistributions must contain a verbatim copy of this document.

The OpenLDAP Foundation may revise this license from time to time.
Each revision is distinguished by a version number.  You may use
this Software under terms of this license revision or under the
terms of any subsequent revision of the license.

THIS SOFTWARE IS PROVIDED BY THE OPENLDAP FOUNDATION AND ITS
CONTRIBUTORS ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT
SHALL THE OPENLDAP FOUNDATION, ITS CONTRIBUTORS, OR THE AUTHOR(S)
OR OWNER(S) OF THE SOFTWARE BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

The names of the authors and copyright holders must not be used in
advertising or otherwise to promote the sale, use or other dealing
in this Software without specific, written prior permission.  Title
to copyright in this Software shall at all times remain with copyright
holders.

OpenLDAP is a registered trademark of the OpenLDAP Foundation.

Copyright 1999-2003 The OpenLDAP Foundation, Redwood City,
California, USA.  All Rights Reserved.  Permission to copy and
distribute verbatim copies of this document is granted.
