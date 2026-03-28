unless Object.const_defined?(:IOError)
  class IOError < StandardError; end
end

module MDB
  class Stat < Struct.new(:psize, :depth, :branch_pages, :leaf_pages, :overflow_pages, :entries); end

  class Env
    class Info < Struct.new(:mapaddr, :mapsize, :last_pgno, :last_txnid, :maxreaders, :numreaders); end

    def self.new(options = {})
      instance = super()
      options.each do |k, v|
        case k
        when :mapsize    then instance.mapsize = v
        when :maxreaders then instance.maxreaders = v
        when :maxdbs     then instance.maxdbs = v
        else
          raise ArgumentError, "unknown option #{k}"
        end
      end
      instance
    end

    def transaction(*args)
      raise ArgumentError, "no block given" unless block_given?
      txn = Txn.new(self, *args)
      begin
        result = yield txn
        txn.commit
        result
      rescue
        txn.abort
        raise
      end
    end

    def database(*args)
      Database.new(self, *args)
    end
  end

  class Database
    include Enumerable

    attr_reader :dbi

    def initialize(env, *args)
      @env = env
      @dbi = @env.transaction { |txn| Dbi.open(txn, *args) }
    end

    def drop(delete = false)
      @env.transaction { |txn| MDB.drop(txn, @dbi, delete) }
      self
    end

    def flags
      txn = Txn.new(@env, RDONLY)
      Dbi.flags(txn, @dbi)
    ensure
      txn.abort if txn
    end

    def [](key)
      txn = Txn.new(@env, RDONLY)
      MDB.get(txn, @dbi, key)
    ensure
      txn.abort if txn
    end

    # Batch multiple reads in a single RDONLY transaction.
    # Avoids N txn open/close cycles for N reads.
    #
    # The Snapshot object exposes .get(key) which returns copied Strings.
    # The txn is aborted when the block exits.
    #
    #   db.snapshot do |s|          # O(1) — opens one RDONLY txn
    #     a = s.get("key_a")       # O(log n) — one B-tree lookup + memcpy
    #     b = s.get("key_b")       # O(log n) — same txn, consistent view
    #   end                         # O(1) — aborts txn
    #
    def snapshot
      raise ArgumentError, "no block given" unless block_given?
      txn = Txn.new(@env, RDONLY)
      snap = Snapshot.new(txn, @dbi)
      begin
        yield snap
      ensure
        txn.abort
      end
    end

    def fetch(key, default = nil)
      txn = Txn.new(@env, RDONLY)
      val = MDB.get(txn, @dbi, key)
      unless val
        return yield(key) if block_given?
        return default if default
        raise KeyError, "key not found"
      end
      val
    ensure
      txn.abort if txn
    end

    def []=(key, data)
      @env.transaction { |txn| MDB.put(txn, @dbi, key, data) }
      data
    end

    def del(*args)
      @env.transaction { |txn| MDB.del(txn, @dbi, *args) }
      self
    end

    # Iterate all key-value pairs
    def each(&block)
      raise ArgumentError, "no block given" unless block
      txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(txn, @dbi)
      record = cursor.first
      while record
        yield record
        record = cursor.next
      end
      self
    ensure
      cursor.close if cursor
      txn.abort if txn
    end

    # Iterate all records matching a key (DUPSORT databases)
    def each_key(key, &block)
      raise ArgumentError, "no block given" unless block
      txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(txn, @dbi)
      record = cursor.set_key(key)
      while record
        yield record
        record = cursor.next_dup
      end
      self
    ensure
      cursor.close if cursor
      txn.abort if txn
    end

    # Prefix scan: iterate keys starting with the given prefix.
    # Uses MDB_SET_RANGE to seek to the first matching key, then
    # iterates forward while keys still start with the prefix.
    #
    #   db.each_prefix("user:") { |key, val| ... }
    #
    def each_prefix(prefix, &block)
      raise ArgumentError, "no block given" unless block
      txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(txn, @dbi)
      record = cursor.set_range(prefix)
      while record
        key = record[0]
        break unless key.bytesize >= prefix.bytesize &&
                     key.byteslice(0, prefix.bytesize) == prefix
        yield record
        record = cursor.next
      end
      self
    ensure
      cursor.close if cursor
      txn.abort if txn
    end

    def first
      txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(txn, @dbi)
      cursor.first
    ensure
      cursor.close if cursor
      txn.abort if txn
    end

    def last
      txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(txn, @dbi)
      cursor.last
    ensure
      cursor.close if cursor
      txn.abort if txn
    end

    # Batch write: multiple put/del operations in a single transaction.
    #
    #   db.batch do |txn, dbi|
    #     MDB.put(txn, dbi, "key1", "val1")
    #     MDB.put(txn, dbi, "key2", "val2")
    #     MDB.del(txn, dbi, "old_key")
    #   end
    #
    def batch(&block)
      raise ArgumentError, "no block given" unless block
      txn = Txn.new(@env)
      begin
        yield txn, @dbi
        txn.commit
      rescue
        txn.abort
        raise
      end
    end

    # Append-only insert (requires sorted key order)
    def <<(value)
      txn = Txn.new(@env)
      cursor = Cursor.new(txn, @dbi)
      begin
        record = cursor.last
        key = record ? record.first.to_fix.succ : 0
        cursor.put(key.to_bin, value, MDB::APPEND)
        cursor.close
        cursor = nil
        txn.commit
        txn = nil
      rescue
        cursor.close if cursor
        txn.abort if txn
        raise
      end
      self
    end

    # Batch append (requires sorted key order)
    def concat(values)
      txn = Txn.new(@env)
      cursor = Cursor.new(txn, @dbi)
      begin
        record = cursor.last
        key = record ? record.first.to_fix.succ : 0
        values.each do |value|
          cursor.put(key.to_bin, value, MDB::APPEND)
          key += 1
        end
        cursor.close
        cursor = nil
        txn.commit
        txn = nil
      rescue
        cursor.close if cursor
        txn.abort if txn
        raise
      end
      self
    end

    def stat
      txn = Txn.new(@env, RDONLY)
      MDB.stat(txn, @dbi)
    ensure
      txn.abort if txn
    end

    def length
      stat[:entries]
    end
    alias size length

    def empty?
      stat[:entries] == 0
    end

    # Low-level transaction access
    def transaction(*args, &block)
      raise ArgumentError, "no block given" unless block
      txn = Txn.new(@env, *args)
      begin
        result = yield txn, @dbi
        txn.commit
        result
      rescue
        txn.abort
        raise
      end
    end

    # Low-level cursor access
    def cursor(*args, &block)
      raise ArgumentError, "no block given" unless block
      txn = Txn.new(@env, *args)
      cur = Cursor.new(txn, @dbi)
      begin
        result = yield cur
        cur.close
        cur = nil
        txn.commit
        txn = nil
        result
      rescue
        cur.close if cur
        txn.abort if txn
        raise
      end
    end

    def to_a
      ary = []
      if flags & DUPSORT != 0
        each { |key, _| each_key(key) { |k, v| ary << [k, v] } }
      else
        each { |key, value| ary << [key, value] }
      end
      ary
    end

    def to_h
      hsh = {}
      if flags & DUPSORT != 0
        each do |key, _|
          each_key(key) do |k, v|
            hsh[k] ||= []
            hsh[k] << v
          end
        end
      else
        each { |key, value| hsh[key] = value }
      end
      hsh
    end
  end

  class Cursor
    Ops.keys.each do |m|
      define_method(m) do |key = nil, data = nil|
        get(Ops[m], key, data)
      end
    end
  end

  # Snapshot: a thin wrapper that holds a RDONLY txn + dbi.
  # Exposed only through Database#snapshot { |s| ... }
  # All gets return copied Strings. The txn is not exposed.
  class Snapshot
    def initialize(txn, dbi)
      @txn = txn
      @dbi = dbi
    end

    # O(log n) B-tree lookup. Returns copied String or nil.
    def get(key)
      MDB.get(@txn, @dbi, key)
    end
  end
end
