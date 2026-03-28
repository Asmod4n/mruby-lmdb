LMDB_TEST_TMP = "/tmp"

Dir.entries(LMDB_TEST_TMP).each do |name|
  next unless name.start_with?("mruby-lmdb-test-")

  path = "#{LMDB_TEST_TMP}/#{name}"
  lock = "#{path}-lock"

  # Try to delete both DB file and lock file
  begin
    File.delete(path) if File.exist?(path)
  rescue => e
    # ignore, just like your existing cleanup
  end

  begin
    File.delete(lock) if File.exist?(lock)
  rescue => e
    # ignore
  end
end

def with_test_db(flags = 0, maxdbs: 4, &block)
  path = "#{LMDB_TEST_TMP}/mruby-lmdb-test-#{$$}-#{rand(100000)}"
  env = MDB::Env.new(mapsize: 10485760, maxdbs: maxdbs)
  env.open(path, MDB::NOSUBDIR | flags)
  yield env
ensure
  env.close rescue nil
  File.delete(path) rescue nil
  File.delete("#{path}-lock") rescue nil
end

assert('Env.new no options') do
  with_test_db { |env| assert_true env.is_a?(MDB::Env) }
end

assert('Env.new mapsize option') do
  with_test_db { |env| assert_true env.info[:mapsize] >= 10485760 }
end

assert('Env.new maxdbs option') do
  with_test_db(maxdbs: 8) { |env| assert_true env.is_a?(MDB::Env) }
end

assert('Env.new unknown symbol option raises ArgumentError') do
  assert_raise(ArgumentError) { MDB::Env.new(bogus: 1) }
end

assert('Env.new string key option raises ArgumentError') do
  assert_raise(ArgumentError) { MDB::Env.new("mapsize" => 1024) }
end

assert('Env.new integer key option raises ArgumentError') do
  assert_raise(ArgumentError) { MDB::Env.new(42 => 1024) }
end

assert('Env.new negative mapsize raises RangeError') do
  assert_raise(RangeError) { MDB::Env.new(mapsize: -1) }
end

assert('Env.new negative maxreaders raises RangeError') do
  assert_raise(RangeError) { MDB::Env.new(maxreaders: -1) }
end

assert('Env.new negative maxdbs raises RangeError') do
  assert_raise(RangeError) { MDB::Env.new(maxdbs: -1) }
end

assert('Env#stat returns MDB::Stat') do
  with_test_db do |env|
    s = env.stat
    assert_true s.is_a?(MDB::Stat)
    assert_true s[:psize] > 0
  end
end

assert('Env#info returns MDB::Env::Info') do
  with_test_db do |env|
    i = env.info
    assert_true i.is_a?(MDB::Env::Info)
    assert_true i[:mapsize] > 0
  end
end

assert('Env#path contains opened path') do
  with_test_db { |env| assert_true env.path.include?("mruby-lmdb-test") }
end

assert('Env#maxkeysize positive') do
  with_test_db { |env| assert_true env.maxkeysize > 0 }
end

assert('Env#flags returns integer') do
  with_test_db { |env| assert_true env.flags.is_a?(Integer) }
end

assert('Env#reader_check returns non-negative integer') do
  with_test_db do |env|
    dead = env.reader_check
    assert_true dead.is_a?(Integer) && dead >= 0
  end
end

assert('Env methods raise IOError after close') do
  with_test_db do |env|
    env.close
    assert_raise(IOError) { env.stat }
    assert_raise(IOError) { env.info }
    assert_raise(IOError) { env.path }
    assert_raise(IOError) { env.flags }
    assert_raise(IOError) { env.maxkeysize }
    assert_raise(IOError) { env.reader_check }
    assert_raise(IOError) { env.database }
    assert_raise(IOError) { env.transaction { } }
    assert_raise(IOError) { env.sync }
  end
end

assert('Env#close is idempotent') do
  with_test_db do |env|
    assert_true  env.close
    assert_false env.close
    assert_false env.close
  end
end

assert('Env#mapsize= negative raises RangeError') do
  with_test_db { |env| assert_raise(RangeError) { env.mapsize = -1 } }
end

assert('Env#maxreaders= negative raises RangeError') do
  with_test_db { |env| assert_raise(RangeError) { env.maxreaders = -1 } }
end

assert('Env#maxdbs= negative raises RangeError') do
  with_test_db { |env| assert_raise(RangeError) { env.maxdbs = -1 } }
end

assert('Env#mapsize= wrong type raises TypeError') do
  with_test_db { |env| assert_raise(TypeError) { env.mapsize = "big" } }
end

assert('Env#transaction commits on success') do
  with_test_db do |env|
    db = env.database
    env.transaction { |txn| MDB.put(txn, db.dbi, "k", "v") }
    assert_equal "v", db["k"]
  end
end

assert('Env#transaction aborts and re-raises on exception') do
  with_test_db do |env|
    db = env.database
    assert_raise(RuntimeError) do
      env.transaction do |txn|
        MDB.put(txn, db.dbi, "k", "v")
        raise "boom"
      end
    end
    assert_nil db["k"]
  end
end

assert('Env#transaction without block raises ArgumentError') do
  with_test_db { |env| assert_raise(ArgumentError) { env.transaction } }
end

assert('Env#transaction RDONLY rejects writes') do
  with_test_db do |env|
    db = env.database
    assert_raise(SystemCallError) do
      env.transaction(MDB::RDONLY) { |txn| MDB.put(txn, db.dbi, "k", "v") }
    end
  end
end

assert('Env#transaction wrong flag type raises TypeError') do
  with_test_db { |env| assert_raise(TypeError) { env.transaction("rdonly") { } } }
end

assert('Env#sync without args succeeds') do
  with_test_db { |env| assert_equal env, env.sync }
end

assert('Env#sync force=true succeeds') do
  with_test_db { |env| assert_equal env, env.sync(true) }
end

assert('Env#sync with non-bool arg is accepted') do
  with_test_db { |env| assert_equal env, env.sync(1) }
end

assert('Env#copy copies the database file') do
  with_test_db do |env|
    db = env.database
    db["k"] = "v"
    dest = "#{LMDB_TEST_TMP}/mruby-lmdb-copy-#{$$}"
    env.copy(dest)
    assert_true File.exist?(dest)
    File.delete(dest) rescue nil
    File.delete("#{dest}-lock") rescue nil
  end
end

assert('Env#copy wrong type raises TypeError') do
  with_test_db { |env| assert_raise(TypeError) { env.copy(42) } }
end

assert('Database opens unnamed db') do
  with_test_db { |env| assert_true env.database.is_a?(MDB::Database) }
end

assert('Database opens named db') do
  with_test_db do |env|
    db = env.database(MDB::CREATE, "mydb")
    assert_true db.is_a?(MDB::Database)
  end
end

assert('Database#dbi returns non-negative integer') do
  with_test_db do |env|
    db = env.database
    assert_true db.dbi.is_a?(Integer) && db.dbi >= 0
  end
end

assert('Database put and get roundtrip') do
  with_test_db do |env|
    db = env.database
    db["hello"] = "world"
    assert_equal "world", db["hello"]
  end
end

assert('Database get missing key returns nil') do
  with_test_db { |env| assert_nil env.database["nope"] }
end

assert('Database overwrite key') do
  with_test_db do |env|
    db = env.database
    db["k"] = "v1"
    db["k"] = "v2"
    assert_equal "v2", db["k"]
  end
end

assert('Database binary key and value roundtrip') do
  with_test_db do |env|
    db = env.database
    db["\x00\x01\x02"] = "\xFF\xFE\xFD"
    assert_equal "\xFF\xFE\xFD", db["\x00\x01\x02"]
  end
end

assert('Database empty string key raises MDB::BAD_VALSIZE') do
  with_test_db do |env|
    assert_raise(MDB::BAD_VALSIZE) { env.database[""] = "v" }
  end
end

assert('Database large value roundtrip') do
  with_test_db do |env|
    db = env.database
    val = "x" * 100000
    db["big"] = val
    assert_equal val, db["big"]
  end
end

assert('Database#del removes key') do
  with_test_db do |env|
    db = env.database
    db["k"] = "v"
    db.del("k")
    assert_nil db["k"]
  end
end

assert('Database#del missing key is silent') do
  with_test_db do |env|
    env.database.del("nonexistent")
    assert_true true
  end
end

assert('Database#fetch existing key') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    assert_equal "1", db.fetch("a")
  end
end

assert('Database#fetch missing key with default') do
  with_test_db { |env| assert_equal "def", env.database.fetch("x", "def") }
end

assert('Database#fetch missing key with block') do
  with_test_db do |env|
    result = env.database.fetch("x") { |k| "got_#{k}" }
    assert_equal "got_x", result
  end
end

assert('Database#fetch missing key no default raises KeyError') do
  with_test_db { |env| assert_raise(KeyError) { env.database.fetch("x") } }
end

assert('Database#stat returns MDB::Stat') do
  with_test_db { |env| assert_true env.database.stat.is_a?(MDB::Stat) }
end

assert('Database#length and #empty?') do
  with_test_db do |env|
    db = env.database
    assert_equal 0, db.length
    assert_true  db.empty?
    db["x"] = "y"
    assert_equal 1, db.length
    assert_false db.empty?
  end
end

assert('Database#flags returns integer') do
  with_test_db { |env| assert_true env.database.flags.is_a?(Integer) }
end

assert('Database#drop empties db') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"; db["b"] = "2"
    db.drop
    assert_equal 0, db.length
  end
end

assert('Database#first and #last') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"; db["b"] = "2"; db["c"] = "3"
    assert_equal "a", db.first[0]
    assert_equal "c", db.last[0]
  end
end

assert('Database#first on empty db returns nil') do
  with_test_db { |env| assert_nil env.database.first }
end

assert('Database#last on empty db returns nil') do
  with_test_db { |env| assert_nil env.database.last }
end

assert('Database#each iterates all records in key order') do
  with_test_db do |env|
    db = env.database
    db["b"] = "2"; db["a"] = "1"; db["c"] = "3"
    keys = []
    db.each { |k, v| keys << k }
    assert_equal ["a", "b", "c"], keys
  end
end

assert('Database#each on empty db yields nothing') do
  with_test_db do |env|
    count = 0
    env.database.each { count += 1 }
    assert_equal 0, count
  end
end

assert('Database#each without block raises ArgumentError') do
  with_test_db { |env| assert_raise(ArgumentError) { env.database.each } }
end

assert('Database#each exception propagates after cleanup') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"; db["b"] = "2"
    seen = []
    begin
      db.each { |k, v| seen << k; raise "stop" if k == "a" }
    rescue RuntimeError => e
      assert_equal "stop", e.message
    end
    assert_equal ["a"], seen
  end
end

assert('Database#each_prefix finds matching keys') do
  with_test_db do |env|
    db = env.database
    db["user:1"] = "Alice"; db["user:2"] = "Bob"; db["post:1"] = "X"
    users = []
    db.each_prefix("user:") { |k, v| users << v }
    assert_equal ["Alice", "Bob"], users
  end
end

assert('Database#each_prefix no matches yields nothing') do
  with_test_db do |env|
    db = env.database
    db["abc"] = "1"
    count = 0
    db.each_prefix("xyz") { count += 1 }
    assert_equal 0, count
  end
end

assert('Database#each_prefix empty prefix raises MDB::BAD_VALSIZE') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    assert_raise(MDB::BAD_VALSIZE) { db.each_prefix("") { } }
  end
end

assert('Database#each_prefix without block raises ArgumentError') do
  with_test_db { |env| assert_raise(ArgumentError) { env.database.each_prefix("x") } }
end

assert('Database#each_prefix exception propagates') do
  with_test_db do |env|
    db = env.database
    db["a:1"] = "1"; db["a:2"] = "2"
    seen = []
    begin
      db.each_prefix("a:") { |k, v| seen << k; raise "stop" }
    rescue RuntimeError => e
      assert_equal "stop", e.message
    end
    assert_equal ["a:1"], seen
  end
end

assert('Database#batch commits on success') do
  with_test_db do |env|
    db = env.database
    db.batch do |txn, dbi|
      MDB.put(txn, dbi, "k1", "v1")
      MDB.put(txn, dbi, "k2", "v2")
    end
    assert_equal "v1", db["k1"]
    assert_equal "v2", db["k2"]
  end
end

assert('Database#batch aborts and re-raises on exception') do
  with_test_db do |env|
    db = env.database
    assert_raise(RuntimeError) do
      db.batch do |txn, dbi|
        MDB.put(txn, dbi, "k1", "v1")
        raise "boom"
      end
    end
    assert_nil db["k1"]
  end
end

assert('Database#batch without block raises ArgumentError') do
  with_test_db { |env| assert_raise(ArgumentError) { env.database.batch } }
end

assert('Database#transaction commits on success') do
  with_test_db do |env|
    db = env.database
    db.transaction { |txn, dbi| MDB.put(txn, dbi, "k", "v") }
    assert_equal "v", db["k"]
  end
end

assert('Database#transaction RDONLY allows reads') do
  with_test_db do |env|
    db = env.database
    db["k"] = "v"
    result = nil
    db.transaction(MDB::RDONLY) { |txn, dbi| result = MDB.get(txn, dbi, "k") }
    assert_equal "v", result
  end
end

assert('Database#transaction RDONLY rejects writes') do
  with_test_db do |env|
    db = env.database
    assert_raise(SystemCallError) do
      db.transaction(MDB::RDONLY) { |txn, dbi| MDB.put(txn, dbi, "k", "v") }
    end
  end
end

assert('Database#transaction without block raises ArgumentError') do
  with_test_db { |env| assert_raise(ArgumentError) { env.database.transaction } }
end

assert('Database#transaction wrong flag type raises TypeError') do
  with_test_db { |env| assert_raise(TypeError) { env.database.transaction("rdonly") { } } }
end

assert('Database#cursor iterates forward') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"; db["b"] = "2"
    results = []
    db.cursor(MDB::RDONLY) do |c|
      r = c.first
      while r
        results << r[0]
        r = c.next
      end
    end
    assert_equal ["a", "b"], results
  end
end

assert('Database#cursor set_range seeks to prefix') do
  with_test_db do |env|
    db = env.database
    db["aaa"] = "1"; db["bbb"] = "2"; db["bbc"] = "3"; db["ccc"] = "4"
    result = nil
    db.cursor(MDB::RDONLY) { |c| result = c.set_range("bb") }
    assert_not_nil result
    assert_equal "bbb", result[0]
  end
end

assert('Database#cursor close is idempotent') do
  with_test_db do |env|
    db = env.database
    db.cursor(MDB::RDONLY) do |c|
      assert_true  c.close
      assert_false c.close
    end
  end
end

assert('Database#cursor without block raises ArgumentError') do
  with_test_db { |env| assert_raise(ArgumentError) { env.database.cursor } }
end

assert('Cursor get on closed cursor raises RuntimeError') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    db.cursor(MDB::RDONLY) do |c|
      c.close
      assert_raise(RuntimeError) { c.first }
    end
  end
end

assert('Cursor wrong op raises RangeError') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    db.cursor(MDB::RDONLY) { |c| assert_raise(RangeError) { c.get(99999) } }
  end
end

assert('Cursor#first / next / last / prev navigate correctly') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"; db["b"] = "2"; db["c"] = "3"
    db.cursor(MDB::RDONLY) do |c|
      assert_equal "a", c.first[0]
      assert_equal "b", c.next[0]
      assert_equal "c", c.last[0]
      assert_equal "b", c.prev[0]
    end
  end
end

assert('Cursor#set positions on exact key') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"; db["b"] = "2"; db["c"] = "3"
    db.cursor(MDB::RDONLY) do |c|
      r = c.set("b")
      assert_not_nil r
      assert_equal "b", r[0]
    end
  end
end

assert('Cursor#set on missing key returns nil') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    db.cursor(MDB::RDONLY) { |c| assert_nil c.set("missing") }
  end
end

assert('Cursor#next at end returns nil') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    db.cursor(MDB::RDONLY) { |c| c.last; assert_nil c.next }
  end
end

assert('Cursor#prev at start returns nil') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    db.cursor(MDB::RDONLY) { |c| c.first; assert_nil c.prev }
  end
end

assert('Cursor#put writes a record') do
  with_test_db do |env|
    db = env.database
    db.cursor { |c| c.put("hello", "world") }
    assert_equal "world", db["hello"]
  end
end

assert('Cursor#put NOOVERWRITE raises on existing key') do
  with_test_db do |env|
    db = env.database
    db["k"] = "v"
    assert_raise(MDB::Error) do
      db.cursor { |c| c.put("k", "v2", MDB::NOOVERWRITE) }
    end
  end
end

assert('Cursor#del removes current record') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"; db["b"] = "2"
    db.cursor do |c|
      c.first
      c.del
    end
    assert_nil db["a"]
    assert_equal "2", db["b"]
  end
end

assert('Cursor#del on closed cursor raises RuntimeError') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    db.cursor do |c|
      c.first
      c.close
      assert_raise(RuntimeError) { c.del }
    end
  end
end

assert('Cursor#count on DUPSORT key returns dup count') do
  with_test_db do |env|
    db = env.database(MDB::DUPSORT)
    env.transaction do |txn|
      MDB.put(txn, db.dbi, "k", "a")
      MDB.put(txn, db.dbi, "k", "b")
      MDB.put(txn, db.dbi, "k", "c")
    end
    count = nil
    db.cursor(MDB::RDONLY) { |c| c.set("k"); count = c.count }
    assert_equal 3, count
  end
end

assert('Txn.new requires env argument') do
  assert_raise(ArgumentError) { MDB::Txn.new }
end

assert('Txn.new wrong type raises IOError') do
  with_test_db { |env| assert_raise(IOError) { MDB::Txn.new("not an env") } }
end

assert('Txn commit is final') do
  with_test_db do |env|
    txn = MDB::Txn.new(env)
    txn.commit
    assert_raise(RuntimeError) { txn.commit }
  end
end

assert('Txn abort is idempotent') do
  with_test_db do |env|
    txn = MDB::Txn.new(env)
    assert_true  txn.abort
    assert_false txn.abort
  end
end

assert('Txn abort after commit returns false') do
  with_test_db do |env|
    txn = MDB::Txn.new(env)
    txn.commit
    assert_false txn.abort
  end
end

assert('Txn#reset and renew on RDONLY txn') do
  with_test_db do |env|
    db = env.database
    db["k"] = "v"
    txn = MDB::Txn.new(env, MDB::RDONLY)
    assert_equal "v", MDB.get(txn, db.dbi, "k")
    txn.reset
    txn.renew
    assert_equal "v", MDB.get(txn, db.dbi, "k")
    txn.abort
  end
end

assert('Txn#reset on write txn does not raise') do
  with_test_db do |env|
    txn = MDB::Txn.new(env)
    assert_equal txn, txn.reset
    txn.abort
  end
end

assert('MDB.get returns value or nil') do
  with_test_db do |env|
    db = env.database
    db["k"] = "v"
    env.transaction(MDB::RDONLY) do |txn|
      assert_equal "v", MDB.get(txn, db.dbi, "k")
      assert_nil        MDB.get(txn, db.dbi, "missing")
    end
  end
end

assert('MDB.get wrong txn type raises RuntimeError') do
  with_test_db do |env|
    db = env.database
    assert_raise(RuntimeError) { MDB.get("not a txn", db.dbi, "k") }
  end
end

assert('MDB.put and MDB.del roundtrip') do
  with_test_db do |env|
    db = env.database
    env.transaction { |txn| MDB.put(txn, db.dbi, "k", "v") }
    assert_equal "v", db["k"]
    env.transaction { |txn| MDB.del(txn, db.dbi, "k") }
    assert_nil db["k"]
  end
end

assert('MDB.put NOOVERWRITE raises MDB::KEYEXIST') do
  with_test_db do |env|
    db = env.database
    db["k"] = "v1"
    assert_raise(MDB::KEYEXIST) do
      env.transaction { |txn| MDB.put(txn, db.dbi, "k", "v2", MDB::NOOVERWRITE) }
    end
  end
end

assert('MDB.del missing key returns nil') do
  with_test_db do |env|
    db = env.database
    env.transaction { |txn| assert_nil MDB.del(txn, db.dbi, "missing") }
  end
end

assert('MDB.stat returns MDB::Stat') do
  with_test_db do |env|
    db = env.database
    env.transaction(MDB::RDONLY) do |txn|
      assert_true MDB.stat(txn, db.dbi).is_a?(MDB::Stat)
    end
  end
end

assert('MDB.get/put/del on committed txn raises RuntimeError') do
  with_test_db do |env|
    db = env.database
    txn = MDB::Txn.new(env)
    txn.commit
    assert_raise(RuntimeError) { MDB.get(txn, db.dbi, "k") }
    assert_raise(RuntimeError) { MDB.put(txn, db.dbi, "k", "v") }
    assert_raise(RuntimeError) { MDB.del(txn, db.dbi, "k") }
  end
end

assert('MDB.drop empties database') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"; db["b"] = "2"
    env.transaction { |txn| MDB.drop(txn, db.dbi) }
    assert_equal 0, db.length
  end
end

assert('MDB.drop wrong txn type raises RuntimeError') do
  with_test_db do |env|
    db = env.database
    assert_raise(RuntimeError) { MDB.drop("nope", db.dbi) }
  end
end

assert('MDB::Error is a RuntimeError') do
  assert_true MDB::Error.ancestors.include?(RuntimeError)
end

assert('MDB::NOTFOUND is an MDB::Error subclass') do
  assert_true MDB::NOTFOUND.ancestors.include?(MDB::Error)
end

assert('MDB::KEYEXIST is an MDB::Error subclass') do
  assert_true MDB::KEYEXIST.ancestors.include?(MDB::Error)
end

assert('MDB::KEYEXIST can be rescued as MDB::Error') do
  with_test_db do |env|
    db = env.database
    db["k"] = "v"
    rescued = false
    begin
      env.transaction { |txn| MDB.put(txn, db.dbi, "k", "v2", MDB::NOOVERWRITE) }
    rescue MDB::Error
      rescued = true
    end
    assert_true rescued
  end
end

assert('MDB::KEYEXIST can be rescued specifically') do
  with_test_db do |env|
    db = env.database
    db["k"] = "v"
    rescued = false
    begin
      env.transaction { |txn| MDB.put(txn, db.dbi, "k", "v2", MDB::NOOVERWRITE) }
    rescue MDB::KEYEXIST
      rescued = true
    end
    assert_true rescued
  end
end

assert('Database#multi_get returns values in input key order') do
  with_test_db do |env|
    db = env.database
    db["c"] = "3"; db["a"] = "1"; db["b"] = "2"
    assert_equal ["2", "1", "3", nil], db.multi_get(["b", "a", "c", "missing"])
  end
end

assert('Database#multi_get empty array returns empty array') do
  with_test_db { |env| assert_equal [], env.database.multi_get([]) }
end

assert('Database#multi_get wrong type raises TypeError') do
  with_test_db { |env| assert_raise(TypeError) { env.database.multi_get("not an array") } }
end

assert('Database#batch_put inserts all pairs') do
  with_test_db do |env|
    db = env.database
    db.batch_put([["z", "26"], ["a", "1"], ["m", "13"]])
    keys = []
    db.each { |k, _v| keys << k }
    assert_equal ["a", "m", "z"], keys
  end
end

assert('Database#batch_put empty array is valid') do
  with_test_db do |env|
    env.database.batch_put([])
    assert_true true
  end
end

assert('Database#batch_put wrong type raises TypeError') do
  with_test_db { |env| assert_raise(TypeError) { env.database.batch_put("oops") } }
end

assert('Database#batch_put NOOVERWRITE raises MDB::KEYEXIST on duplicate') do
  with_test_db do |env|
    db = env.database
    db["a"] = "old"
    assert_raise(MDB::KEYEXIST) { db.batch_put([["a", "new"]], MDB::NOOVERWRITE) }
    assert_equal "old", db["a"]
  end
end

assert('Database#<< appends with auto-increment keys') do
  with_test_db do |env|
    db = env.database(MDB::INTEGERKEY)
    db << "hello" << "world"
    assert_equal 2, db.length
  end
end

assert('Database#concat batch appends in order') do
  with_test_db do |env|
    db = env.database(MDB::INTEGERKEY)
    db.concat(["a", "b", "c"])
    vals = []
    db.each { |k, v| vals << v }
    assert_equal ["a", "b", "c"], vals
  end
end

assert('Database#concat continues from last key') do
  with_test_db do |env|
    db = env.database(MDB::INTEGERKEY)
    db << "first"
    db.concat(["second", "third"])
    keys = []
    db.each { |k, _v| keys << k.to_fix }
    assert_equal [0, 1, 2], keys
  end
end

assert('Database#concat wrong type raises TypeError') do
  with_test_db do |env|
    db = env.database(MDB::INTEGERKEY)
    assert_raise(TypeError) { db.concat("not an array") }
  end
end

assert('Database#to_a returns all pairs') do
  with_test_db do |env|
    db = env.database
    db["x"] = "1"; db["y"] = "2"
    assert_equal 2, db.to_a.length
  end
end

assert('Database#to_h returns correct hash') do
  with_test_db do |env|
    db = env.database
    db["x"] = "1"; db["y"] = "2"
    h = db.to_h
    assert_equal "1", h["x"]
    assert_equal "2", h["y"]
  end
end

assert('Database#to_a on empty db returns empty array') do
  with_test_db { |env| assert_equal [], env.database.to_a }
end

assert('Database#to_h on empty db returns empty hash') do
  with_test_db { |env| assert_equal({}, env.database.to_h) }
end

assert('Database#each_key iterates duplicate values for a key') do
  with_test_db do |env|
    db = env.database(MDB::DUPSORT)
    env.transaction do |txn|
      MDB.put(txn, db.dbi, "k", "a")
      MDB.put(txn, db.dbi, "k", "b")
      MDB.put(txn, db.dbi, "k", "c")
    end
    vals = []
    db.each_key("k") { |k, v| vals << v }
    assert_equal ["a", "b", "c"], vals
  end
end

assert('Database#each_key on missing key yields nothing') do
  with_test_db do |env|
    db = env.database(MDB::DUPSORT)
    count = 0
    db.each_key("missing") { count += 1 }
    assert_equal 0, count
  end
end

assert('Database#each_key without block raises ArgumentError') do
  with_test_db do |env|
    db = env.database(MDB::DUPSORT)
    assert_raise(ArgumentError) { db.each_key("k") }
  end
end

assert('to_bin / to_fix roundtrip') do
  [0, 1, -1, 42, 255, 256, 65535, 65536, -32768, 1000000, -1000000].each do |n|
    assert_equal n, n.to_bin.to_fix
  end
end

assert('to_bin produces fixed-width strings') do
  width = 0.to_bin.bytesize
  [0, 1, -1, 255, 65536, 1000000].each { |n| assert_equal width, n.to_bin.bytesize }
end

assert('to_fix rejects wrong-sized strings') do
  assert_raise(TypeError) { "".to_fix }
  assert_raise(TypeError) { "x".to_fix }
  assert_raise(TypeError) { ("x" * 100).to_fix }
end

assert('Integer keys sort in numeric order with MDB_INTEGERKEY') do
  with_test_db do |env|
    db = env.database(MDB::INTEGERKEY)
    [100, 1, 50, 10, 1000, 0, 255, 256].each { |n| db[n.to_bin] = n.to_s }
    keys = []
    db.each { |k, _v| keys << k.to_fix }
    assert_equal [0, 1, 10, 50, 100, 255, 256, 1000], keys
  end
end

assert('String keys sort lexicographically') do
  with_test_db do |env|
    db = env.database
    ["banana", "apple", "cherry", "date", "apricot"].each { |s| db[s] = "1" }
    keys = []
    db.each { |k, _v| keys << k }
    assert_equal ["apple", "apricot", "banana", "cherry", "date"], keys
  end
end

assert('Numeric strings sort lexicographically not numerically') do
  with_test_db do |env|
    db = env.database
    ["9", "10", "1", "100", "2", "20"].each { |s| db[s] = "1" }
    keys = []
    db.each { |k, _v| keys << k }
    assert_equal ["1", "10", "100", "2", "20", "9"], keys
  end
end

assert('Enumerable#map works on Database') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"; db["b"] = "2"
    result = db.map { |k, v| "#{k}=#{v}" }
    assert_equal ["a=1", "b=2"], result
  end
end

assert('Enumerable#select works on Database') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"; db["b"] = "2"; db["c"] = "3"
    result = db.select { |k, v| v.to_i > 1 }
    assert_equal 2, result.length
  end
end

assert('MDB::Dbi.open returns integer dbi') do
  with_test_db do |env|
    txn = MDB::Txn.new(env)
    dbi = MDB::Dbi.open(txn, 0)
    assert_true dbi.is_a?(Integer)
    txn.commit
  end
end

assert('MDB::Dbi.flags returns integer') do
  with_test_db do |env|
    db = env.database
    env.transaction(MDB::RDONLY) do |txn|
      assert_true MDB::Dbi.flags(txn, db.dbi).is_a?(Integer)
    end
  end
end

assert('MDB::Dbi.open wrong txn type raises RuntimeError') do
  with_test_db { |env| assert_raise(RuntimeError) { MDB::Dbi.open("nope", 0) } }
end

assert('MDB.get requires 3 args') do
  with_test_db do |env|
    db = env.database
    env.transaction(MDB::RDONLY) do |txn|
      assert_raise(ArgumentError) { MDB.get(txn, db.dbi) }
    end
  end
end

assert('MDB.put requires at least 4 args') do
  with_test_db do |env|
    db = env.database
    env.transaction do |txn|
      assert_raise(ArgumentError) { MDB.put(txn, db.dbi, "k") }
    end
  end
end

assert('MDB.del requires at least 3 args') do
  with_test_db do |env|
    db = env.database
    env.transaction do |txn|
      assert_raise(ArgumentError) { MDB.del(txn, db.dbi) }
    end
  end
end

assert('Env#open requires at least 1 arg') do
  env = MDB::Env.new
  assert_raise(ArgumentError) { env.open }
  env.close rescue nil
end

assert('Database#batch_put requires 1 arg') do
  with_test_db { |env| assert_raise(ArgumentError) { env.database.batch_put } }
end

assert('Database#multi_get requires 1 arg') do
  with_test_db { |env| assert_raise(ArgumentError) { env.database.multi_get } }
end

assert('Database#each_prefix requires 1 arg') do
  with_test_db { |env| assert_raise(ArgumentError) { env.database.each_prefix } }
end

assert('Database#each_key requires 1 arg') do
  with_test_db do |env|
    assert_raise(ArgumentError) { env.database(MDB::DUPSORT).each_key }
  end
end

assert('MDB.multi_get returns values in input order') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"; db["b"] = "2"; db["c"] = "3"
    env.transaction(MDB::RDONLY) do |txn|
      result = MDB.multi_get(txn, db.dbi, ["c", "a", "missing", "b"])
      assert_equal ["3", "1", nil, "2"], result
    end
  end
end

assert('MDB.batch_put inserts all pairs in one txn') do
  with_test_db do |env|
    db = env.database
    env.transaction { |txn| MDB.batch_put(txn, db.dbi, [["x", "1"], ["y", "2"]]) }
    assert_equal "1", db["x"]
    assert_equal "2", db["y"]
  end
end

assert('Database accepts integer key via to_s coercion') do
  with_test_db do |env|
    db = env.database
    db[42] = "forty-two"
    assert_equal "forty-two", db["42"]
  end
end

assert('Database accepts symbol key via to_s coercion') do
  with_test_db do |env|
    db = env.database
    db[:hello] = "world"
    assert_equal "world", db["hello"]
  end
end

assert('MDB.put accepts integer key via to_s coercion') do
  with_test_db do |env|
    db = env.database
    env.transaction { |txn| MDB.put(txn, db.dbi, 42, "v") }
    assert_equal "v", db["42"]
  end
end

#
# Missing tests for exception‑in‑block behavior
#

assert('Database#each block raise propagates and cleans up') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    db["b"] = "2"

    seen = []
    begin
      db.each do |k, v|
        seen << k
        raise "boom" if k == "a"
      end
    rescue => e
      assert_equal "boom", e.message
    end

    assert_equal ["a"], seen
  end
end

assert('Database#each_prefix block raise propagates and cleans up') do
  with_test_db do |env|
    db = env.database
    db["x:1"] = "A"
    db["x:2"] = "B"
    db["y:1"] = "C"

    seen = []
    begin
      db.each_prefix("x:") do |k, v|
        seen << k
        raise "stop" if k == "x:1"
      end
    rescue => e
      assert_equal "stop", e.message
    end

    assert_equal ["x:1"], seen
  end
end

assert('Database#transaction block raise aborts and re-raises') do
  with_test_db do |env|
    db = env.database

    assert_raise(RuntimeError) do
      db.transaction do |txn, dbi|
        MDB.put(txn, db.dbi, "k", "v")
        raise "fail"
      end
    end

    assert_nil db["k"]
  end
end

assert('Database#batch block raise aborts and invalidates txn') do
  with_test_db do |env|
    db = env.database
    captured_txn = nil

    begin
      db.batch do |txn, dbi|
        captured_txn = txn
        raise "boom"
      end
    rescue
    end

    assert_raise(RuntimeError) { MDB.put(captured_txn, db.dbi, "x", "y") }
  end
end

assert('Database#cursor block raise closes cursor and re-raises') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"

    closed_cursor = nil

    assert_raise(RuntimeError) do
      db.cursor(MDB::RDONLY) do |c|
        closed_cursor = c
        raise "explode"
      end
    end

    assert_raise(RuntimeError) { closed_cursor.first }
  end
end

assert('Cursor iteration block raise cleans up cursor') do
  with_test_db do |env|
    db = env.database
    db["a"] = "1"
    db["b"] = "2"

    cursor_ref = nil

    begin
      db.cursor(MDB::RDONLY) do |c|
        cursor_ref = c
        r = c.first
        while r
          raise "stop" if r[0] == "a"
          r = c.next
        end
      end
    rescue => e
      assert_equal "stop", e.message
    end

    assert_raise(RuntimeError) { cursor_ref.next }
  end
end
