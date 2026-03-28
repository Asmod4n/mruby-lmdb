N        = 10_000
VAL_SIZE = 100
MAPSIZE  = 256 * 1024 * 1024

def now_ms
  Time.now.to_f * 1000.0
end

def fmt(n, ms)
  ops = (n / (ms / 1000.0)).to_i
  "#{ms.to_i.to_s.rjust(8)} ms  (#{ops.to_s.rjust(7)} ops/s)"
end

path  = "/tmp/bench-mruby-lmdb-#{$$}"
value = "x" * VAL_SIZE

env = MDB::Env.new(mapsize: MAPSIZE)
env.open(path, MDB::NOSUBDIR | MDB::NOSYNC | MDB::NOMETASYNC)
db = env.database

puts "=== mruby LMDB Benchmark (#{N} records, #{VAL_SIZE}-byte values) ==="
puts

# 1. N writes, 1 txn each
t0 = now_ms
N.times { |i| db["key:%08d" % i] = value }
t1 = now_ms
puts "1. Write (1 txn each):   #{fmt(N, t1 - t0)}"

# 2. N writes, 1 txn total
db.drop
t0 = now_ms
env.transaction do |txn|
  dbi = db.dbi
  N.times { |i| MDB.put(txn, dbi, "key:%08d" % i, value) }
end
t1 = now_ms
puts "2. Write (1 txn total):  #{fmt(N, t1 - t0)}"

# 3. N reads, 1 txn each
t0 = now_ms
N.times { |i| db["key:%08d" % i] }
t1 = now_ms
puts "3. Read  (1 txn each):   #{fmt(N, t1 - t0)}"

# 4. N reads, 1 txn total (cursor scan)
count = 0
t0 = now_ms
db.each { count += 1 }
t1 = now_ms
puts "4. Read  (1 txn total):  #{fmt(count, t1 - t0)}"

# 5. Prefix scan
env.transaction do |txn|
  dbi = db.dbi
  N.times { |i| MDB.put(txn, dbi, "user:%06d" % i, value) }
end
count = 0
t0 = now_ms
db.each_prefix("user:") { count += 1 }
t1 = now_ms
puts "5. Prefix scan (#{count}):   #{fmt(count, t1 - t0)}"

env.close
File.delete(path) rescue nil
File.delete("#{path}-lock") rescue nil
