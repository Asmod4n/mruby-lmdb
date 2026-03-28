# **mruby‑lmdb — Fast, Deterministic LMDB Bindings for mruby**

`mruby-lmdb` provides a thin, explicit wrapper around
**Lightning Memory‑Mapped Database (LMDB)**.

It exposes LMDB’s performance model directly:

- **Explicit read/write transactions**
- **Prefix scans, range scans, and cursor operations**
- **Native‑endian integer key encoding (compatible with MDB_INTEGERKEY)**
- **Zero‑magic, predictable behavior**

The goal is to give mruby the same predictable, high‑performance storage semantics that LMDB provides in C.

---

# **Quick Start**

```ruby
env = MDB::Env.new(mapsize: 1_000_000_000)   # 1 GB virtual map
env.open("testdb", MDB::NOSUBDIR)

db = env.database(MDB::INTEGERKEY)

db[18.to_bin] = "hello18"
db << "hello19"
db.concat ["hello20", "hello21"]

db.each do |key, value|
  puts "#{key.to_fix} = #{value}"
end
```

---

# **API Overview**

## **Environment**

```ruby
env = MDB::Env.new(mapsize: 1_000_000_000, maxreaders: 200)
env.open("path", MDB::NOSUBDIR)
```

Options:

| Option | Meaning |
|--------|---------|
| `:mapsize` | Virtual address space reserved for the DB |
| `:maxreaders` | Max concurrent read transactions |
| `:maxdbs` | Max named databases |

### `Env#transaction`

```ruby
env.transaction do |txn|
  # commit on success, abort on exception
end
```

---

## **Database**

```ruby
db = env.database(MDB::INTEGERKEY)
```

### Basic operations

```ruby
db[key]          # read (opens + aborts a read txn)
db[key] = value  # write (one write txn)
db.del(key)
db.length
db.empty?
db.stat
```

---

## **Efficient Reads: `snapshot`**

```ruby
db.snapshot do |s|
  a = s.get("user:1")
  b = s.get("config")
end
```

- One read transaction
- Consistent view
- One string copy per read
- Closes automatically

---

## **Efficient Writes: `batch`**

```ruby
db.batch do |txn, dbi|
  MDB.put(txn, dbi, "k1", "v1")
  MDB.put(txn, dbi, "k2", "v2")
end
```

- One writer lock
- One fsync
- Much faster than individual writes

---

## **Cursors**

### Full scan

```ruby
db.each do |key, value|
  ...
end
```

### Prefix scan

```ruby
db.each_prefix("user:") do |key, value|
  ...
end
```


---

## **Append‑Only Inserts**

Requires sorted keys (e.g., integer keys):

```ruby
db << "value"          # auto-increment key
db.concat ["a", "b"]   # batch append
```

---

# **Performance Model**

LMDB is a B+tree over memory‑mapped pages.

### Operation Costs

| Operation | Time | Allocations | Notes |
|-----------|------|-------------|-------|
| `db["key"]` | O(log n) | 2 | Opens + aborts a txn |
| `db.snapshot` | O(log n) per get | 1 txn + 1 string per get | Best for multi‑read |
| `db.batch` | O(k log n)` | 1 txn | Best for multi‑write |
| `db.each_prefix` | O(log n + m)` | 1 txn + 2 strings per entry | m = matches |

---

# **Reader Slots & Map Size**

LMDB has a fixed number of reader slots (default: 126).
Long‑lived read transactions consume them.

```ruby
env.reader_check   # reclaim dead readers
```

Set a generous mapsize:

```ruby
env = MDB::Env.new(mapsize: 1 << 30)  # 1 GB
```

---

# **License**
Copyright 2015,2026 Hendrik Beskow

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