# Helper: create a temporary LMDB environment for testing
def with_test_db(flags = 0, &block)
  path = "/tmp/mruby-lmdb-test-#{$$}-#{rand(100000)}"
  env = MDB::Env.new(mapsize: 10485760) # 10 MB
  env.open(path, MDB::NOSUBDIR | flags)
  begin
    yield env
  ensure
    env.close
    File.delete(path) rescue nil
    File.delete("#{path}-lock") rescue nil
  end
end

# ============================================================================
# Env basics
# ============================================================================

assert('MDB::Env create and open') do
  with_test_db do |env|
    assert_true env.is_a?(MDB::Env)
  end
end

assert('MDB::Env stat returns Stat struct') do
  with_test_db do |env|
    s = env.stat
    assert_true s.is_a?(MDB::Stat)
    assert_true s[:psize] > 0
  end
end

assert('MDB::Env info returns Info struct') do
  with_test_db do |env|
    i = env.info
    assert_true i.is_a?(MDB::Env::Info)
    assert_true i[:mapsize] > 0
  end
end

assert('MDB::Env path returns the database path') do
  with_test_db do |env|
    p = env.path
    assert_true p.include?("mruby-lmdb-test")
  end
end

assert('MDB::Env maxkeysize is positive') do
  with_test_db do |env|
    assert_true env.maxkeysize > 0
  end
end

assert('MDB::Env close is idempotent') do
  with_test_db do |env|
    assert_true env.close
    assert_false env.close
  end
end

assert('MDB::Env methods raise after close') do
  with_test_db do |env|
    env.close
    assert_raise(IOError) { env.stat }
  end
end

# ============================================================================
# Basic get/put/del
# ============================================================================

assert('MDB::Database put and get') do
  with_test_db do |env|
    db = env.database
    db["hello"] = "world"
    assert_equal "world", db["hello"]
  end
end

assert('MDB::Database get missing key returns nil') do
  with_test_db do |env|
    db = env.database
    assert_nil db["nonexistent"]
  end
end

assert('MDB::Database del removes key') do
  with_test_db do |env|
    db = env.database
    db["key"] = "value"
    assert_equal "value", db["key"]
    db.del("key")
    assert_nil db["key"]
  end
end

assert('MDB::Database multiple put/get') do
  with_test_db do |env|
    db = env.database
    100.times { |i| db["key#{i}"] = "val#{i}" }
    100.times { |i| assert_equal "val#{i}", db["key#{i}"] }
  end
end

assert('MDB::Database overwrite existing key') do
  with_test_db do |env|
    db = env.database
    db["k"] = "v1"
    db["k"] = "v2"
    assert_equal "v2", db["k"]
  end
end

# ============================================================================
# Fetch
# ============================================================================

assert('MDB::Database fetch with existing key') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    assert_equal "1", db.fetch("a")
  end
end

assert('MDB::Database fetch missing key with default') do
  with_test_db do |env|
    db = env.database
    assert_equal "default", db.fetch("missing", "default")
  end
end

assert('MDB::Database fetch missing key with block') do
  with_test_db do |env|
    db = env.database
    result = db.fetch("missing") { |k| "block_#{k}" }
    assert_equal "block_missing", result
  end
end

assert('MDB::Database fetch missing key raises KeyError') do
  with_test_db do |env|
    db = env.database
    assert_raise(KeyError) { db.fetch("missing") }
  end
end

# ============================================================================
# Iteration
# ============================================================================

assert('MDB::Database each iterates all records') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    db["b"] = "2"
    db["c"] = "3"
    pairs = []
    db.each { |k, v| pairs << [k, v] }
    assert_equal 3, pairs.length
    assert_true pairs.include?(["a", "1"])
    assert_true pairs.include?(["b", "2"])
    assert_true pairs.include?(["c", "3"])
  end
end

assert('MDB::Database each on empty db yields nothing') do
  with_test_db do |env|
    db = env.database
    count = 0
    db.each { count += 1 }
    assert_equal 0, count
  end
end

# ============================================================================
# Prefix scan
# ============================================================================

assert('MDB::Database each_prefix finds matching keys') do
  with_test_db do |env|
    db = env.database
    db["user:1"] = "Alice"
    db["user:2"] = "Bob"
    db["user:3"] = "Carol"
    db["post:1"] = "Hello"
    db["post:2"] = "World"

    users = []
    db.each_prefix("user:") { |k, v| users << [k, v] }
    assert_equal 3, users.length
    assert_equal "Alice", users[0][1]

    posts = []
    db.each_prefix("post:") { |k, v| posts << [k, v] }
    assert_equal 2, posts.length
  end
end

assert('MDB::Database each_prefix with no matches yields nothing') do
  with_test_db do |env|
    db = env.database
    db["abc"] = "1"
    count = 0
    db.each_prefix("xyz") { count += 1 }
    assert_equal 0, count
  end
end

# ============================================================================
# first / last
# ============================================================================

assert('MDB::Database first and last') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    db["b"] = "2"
    db["c"] = "3"
    f = db.first
    l = db.last
    assert_equal "a", f[0]
    assert_equal "c", l[0]
  end
end

assert('MDB::Database first on empty db returns nil') do
  with_test_db do |env|
    db = env.database
    assert_nil db.first
  end
end

# ============================================================================
# stat / length / empty?
# ============================================================================

assert('MDB::Database stat reflects entries') do
  with_test_db do |env|
    db = env.database
    assert_equal 0, db.length
    assert_true db.empty?
    db["x"] = "y"
    assert_equal 1, db.length
    assert_false db.empty?
  end
end

# ============================================================================
# Batch / transaction
# ============================================================================

assert('MDB::Database batch writes atomically') do
  with_test_db do |env|
    db = env.database
    db.batch do |txn, dbi|
      MDB.put(txn, dbi, "k1", "v1")
      MDB.put(txn, dbi, "k2", "v2")
      MDB.put(txn, dbi, "k3", "v3")
    end
    assert_equal "v1", db["k1"]
    assert_equal "v2", db["k2"]
    assert_equal "v3", db["k3"]
  end
end

assert('MDB::Database batch rollback on error') do
  with_test_db do |env|
    db = env.database
    begin
      db.batch do |txn, dbi|
        MDB.put(txn, dbi, "k1", "v1")
        raise "deliberate"
      end
    rescue RuntimeError
    end
    assert_nil db["k1"]
  end
end

assert('MDB::Env transaction rollback on error') do
  with_test_db do |env|
    db = env.database
    begin
      env.transaction do |txn|
        MDB.put(txn, db.dbi, "k1", "v1")
        raise "deliberate"
      end
    rescue RuntimeError
    end
    assert_nil db["k1"]
  end
end

# ============================================================================
# Append / concat
# ============================================================================

assert('MDB::Database append with <<') do
  with_test_db do |env|
    db = env.database(MDB::INTEGERKEY)
    db << "hello" << "world"
    assert_equal 2, db.length
  end
end

assert('MDB::Database concat batch append') do
  with_test_db do |env|
    db = env.database(MDB::INTEGERKEY)
    db.concat(["a", "b", "c"])
    assert_equal 3, db.length
  end
end

# ============================================================================
# Cursor
# ============================================================================

assert('MDB::Cursor iterate forward') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    db["b"] = "2"
    results = []
    db.cursor(MDB::RDONLY) do |c|
      r = c.first
      while r
        results << r
        r = c.next
      end
    end
    assert_equal 2, results.length
  end
end

assert('MDB::Cursor set_range for prefix seeking') do
  with_test_db do |env|
    db = env.database
    db["aaa"] = "1"
    db["bbb"] = "2"
    db["bbc"] = "3"
    db["ccc"] = "4"

    result = nil
    db.cursor(MDB::RDONLY) do |c|
      result = c.set_range("bb")
    end
    assert_not_nil result
    assert_equal "bbb", result[0]
  end
end

assert('MDB::Cursor close is idempotent') do
  with_test_db do |env|
    db = env.database
    db.cursor(MDB::RDONLY) do |c|
      c.close
      assert_false c.close
    end
  end
end

# ============================================================================
# Txn lifecycle
# ============================================================================

assert('MDB::Txn commit is final') do
  with_test_db do |env|
    txn = MDB::Txn.new(env)
    txn.commit
    assert_raise(RuntimeError) { txn.commit }
  end
end

assert('MDB::Txn abort is idempotent') do
  with_test_db do |env|
    txn = MDB::Txn.new(env)
    assert_true txn.abort
    assert_false txn.abort
  end
end

assert('MDB::Txn abort after commit returns false') do
  with_test_db do |env|
    txn = MDB::Txn.new(env)
    txn.commit
    assert_false txn.abort
  end
end

# ============================================================================
# Integer key helpers
# ============================================================================

assert('Integer#to_bin / String#to_fix roundtrip') do
  [0, 1, -1, 42, 255, 256, 65535, 65536, 1000000].each do |n|
    assert_equal n, n.to_bin.to_fix
  end
end

assert('Integer#to_bin produces big-endian sorted keys') do
  keys = [0, 1, 2, 100, 1000].map(&:to_bin)
  assert_equal keys, keys.sort
end

# ============================================================================
# Drop
# ============================================================================

assert('MDB::Database drop empties database') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    db["b"] = "2"
    assert_equal 2, db.length
    db.drop
    assert_equal 0, db.length
  end
end

# ============================================================================
# Large values
# ============================================================================

assert('MDB large key-value roundtrip') do
  with_test_db do |env|
    db = env.database
    key = "k"
    val = "x" * 100000
    db[key] = val
    assert_equal val, db[key]
  end
end

# ============================================================================
# to_a / to_h
# ============================================================================

assert('MDB::Database to_a') do
  with_test_db do |env|
    db = env.database
    db["x"] = "1"
    db["y"] = "2"
    ary = db.to_a
    assert_equal 2, ary.length
  end
end

assert('MDB::Database to_h') do
  with_test_db do |env|
    db = env.database
    db["x"] = "1"
    db["y"] = "2"
    h = db.to_h
    assert_equal "1", h["x"]
    assert_equal "2", h["y"]
  end
end

# ============================================================================
# Reader check
# ============================================================================

assert('MDB::Env reader_check returns integer') do
  with_test_db do |env|
    dead = env.reader_check
    assert_true dead.is_a?(Integer)
    assert_true dead >= 0
  end
end

# ============================================================================
# Integer key sort order
# ============================================================================

assert('Integer keys sort in numeric order via MDB_INTEGERKEY') do
  with_test_db do |env|
    db = env.database(MDB::INTEGERKEY)

    # Insert out of order
    [100, 1, 50, 10, 1000, 0, 255, 256].each do |n|
      db[n.to_bin] = "val_#{n}"
    end

    keys = []
    db.each { |k, _v| keys << k.to_fix }
    assert_equal [0, 1, 10, 50, 100, 255, 256, 1000], keys
  end
end

assert('Integer keys: first and last reflect numeric order') do
  with_test_db do |env|
    db = env.database(MDB::INTEGERKEY)

    [500, 1, 9999, 42].each do |n|
      db[n.to_bin] = "val_#{n}"
    end

    assert_equal 1, db.first[0].to_fix
    assert_equal 9999, db.last[0].to_fix
  end
end

assert('Integer keys: sequential append maintains order') do
  with_test_db do |env|
    db = env.database(MDB::INTEGERKEY)

    db << "zero"
    db << "one"
    db << "two"
    db << "three"
    db << "four"

    keys = []
    vals = []
    db.each { |k, v| keys << k.to_fix; vals << v }
    assert_equal [0, 1, 2, 3, 4], keys
    assert_equal ["zero", "one", "two", "three", "four"], vals
  end
end

assert('Integer keys: concat preserves insertion order') do
  with_test_db do |env|
    db = env.database(MDB::INTEGERKEY)

    db.concat(["a", "b", "c", "d", "e"])

    keys = []
    vals = []
    db.each { |k, v| keys << k.to_fix; vals << v }
    assert_equal [0, 1, 2, 3, 4], keys
    assert_equal ["a", "b", "c", "d", "e"], vals
  end
end

assert('Integer keys: concat continues after existing keys') do
  with_test_db do |env|
    db = env.database(MDB::INTEGERKEY)

    db[0.to_bin] = "existing_0"
    db[1.to_bin] = "existing_1"
    db[2.to_bin] = "existing_2"

    db.concat(["new_3", "new_4", "new_5"])

    keys = []
    vals = []
    db.each { |k, v| keys << k.to_fix; vals << v }
    assert_equal [0, 1, 2, 3, 4, 5], keys
    assert_equal ["existing_0", "existing_1", "existing_2", "new_3", "new_4", "new_5"], vals
  end
end

assert('Integer keys: large values sort correctly') do
  with_test_db do |env|
    db = env.database(MDB::INTEGERKEY)

    values = [0, 1, 127, 128, 255, 256, 65535, 65536, 100000]
    values.each { |n| db[n.to_bin] = n.to_s }

    keys = []
    db.each { |k, _v| keys << k.to_fix }
    assert_equal values.sort, keys
  end
end

assert('Integer keys: negative values roundtrip') do
  with_test_db do |env|
    db = env.database(MDB::INTEGERKEY)

    [-1, -100, -32768, 0, 1, 100].each do |n|
      db[n.to_bin] = "val_#{n}"
    end

    keys = []
    db.each { |k, _v| keys << k.to_fix }

    # All keys should roundtrip correctly
    assert_equal 6, keys.length
    [-1, -100, -32768, 0, 1, 100].each do |n|
      assert_true keys.include?(n)
    end
  end
end

assert('to_bin / to_fix roundtrip across range') do
  values = [0, 1, -1, 42, 127, 128, 255, 256, 65535, 65536, -32768, 1000000, -1000000]
  values.each do |n|
    assert_equal n, n.to_bin.to_fix
  end
end

assert('to_bin produces fixed-width strings') do
  [0, 1, -1, 255, 65536, 1000000].each do |n|
    bin = n.to_bin
    # All keys should be sizeof(mrb_int) bytes
    assert_true bin.bytesize == 0.to_bin.bytesize
  end
end

assert('to_fix rejects wrong-sized strings') do
  assert_raise(TypeError) { "".to_fix }
  assert_raise(TypeError) { "x".to_fix }
  assert_raise(TypeError) { "xy".to_fix } if 0.to_bin.bytesize > 2
  assert_raise(TypeError) { ("x" * 100).to_fix }
end

# ============================================================================
# String key lexicographic sort (default DB, no INTEGERKEY)
# ============================================================================

assert('String keys sort lexicographically') do
  with_test_db do |env|
    db = env.database

    ["banana", "apple", "cherry", "date", "apricot"].each do |s|
      db[s] = "1"
    end

    keys = []
    db.each { |k, _v| keys << k }
    assert_equal ["apple", "apricot", "banana", "cherry", "date"], keys
  end
end

assert('String keys: prefix scan returns lexicographic subset') do
  with_test_db do |env|
    db = env.database

    ["user:001", "user:010", "user:002", "user:100", "post:001"].each do |s|
      db[s] = "1"
    end

    keys = []
    db.each_prefix("user:") { |k, _v| keys << k }
    assert_equal ["user:001", "user:002", "user:010", "user:100"], keys
  end
end

assert('String keys: numeric strings sort lexicographically not numerically') do
  with_test_db do |env|
    db = env.database

    ["9", "10", "1", "100", "2", "20"].each do |s|
      db[s] = "1"
    end

    keys = []
    db.each { |k, _v| keys << k }
    # Lexicographic: "1" < "10" < "100" < "2" < "20" < "9"
    assert_equal ["1", "10", "100", "2", "20", "9"], keys
  end
end

assert('String keys: zero-padded numeric strings sort correctly') do
  with_test_db do |env|
    db = env.database

    ["009", "010", "001", "100", "002", "020"].each do |s|
      db[s] = "1"
    end

    keys = []
    db.each { |k, _v| keys << k }
    assert_equal ["001", "002", "009", "010", "020", "100"], keys
  end
end

# ============================================================================
# Bulk operations sort order
# ============================================================================

assert('multi_get returns values in key order of input') do
  with_test_db do |env|
    db = env.database

    db["c"] = "3"
    db["a"] = "1"
    db["b"] = "2"

    result = db.multi_get(["b", "a", "c", "missing"])
    assert_equal ["2", "1", "3", nil], result
  end
end

assert('batch_put maintains correct sort order') do
  with_test_db do |env|
    db = env.database

    pairs = [["z", "26"], ["a", "1"], ["m", "13"]]
    db.batch_put(pairs)

    keys = []
    db.each { |k, _v| keys << k }
    assert_equal ["a", "m", "z"], keys
  end
end
