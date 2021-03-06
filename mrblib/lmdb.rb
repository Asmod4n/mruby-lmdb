module MDB
  class Stat < Struct.new(:psize, :depth, :branch_pages, :leaf_pages, :overflow_pages, :entries); end
  class Env
    class Info < Struct.new(:mapaddr, :mapsize, :last_pgno, :last_txnid, :maxreaders, :numreaders); end

    def self.new(options = {})
      instance = super()
      options.each do |k, v|
        case k
        when :mapsize
          instance.mapsize = v
        when :maxreaders
          instance.maxreaders = v
        when :maxdbs
          instance.maxdbs = v
        else
          raise ArgumentError, "unknown option #{k.dump}"
        end
      end
      instance
    end

    def transaction(*args)
      raise ArgumentError, "no block given" unless block_given?
      txn = Txn.new(self, *args)
      result = yield txn
      txn.commit
      result
    rescue => e
      txn.abort if txn
      raise e
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
      @dbi = @env.transaction {|txn| Dbi.open(txn, *args)}
    end

    def drop(delete = false)
      @env.transaction do |txn|
        MDB.drop(txn, @dbi, delete)
      end
      self
    end

    def flags
      read_txn = Txn.new(@env, RDONLY)
      Dbi.flags(read_txn, @dbi)
    ensure
      read_txn.abort if read_txn
    end

    def [](key)
      read_txn = Txn.new(@env, RDONLY)
      MDB.get(read_txn, @dbi, key)
    ensure
      read_txn.abort if read_txn
    end

    def fetch(key, default = nil)
      read_txn = Txn.new(@env, RDONLY)
      val = MDB.get(read_txn, @dbi, key)
      unless val
        return yield(key) if block_given?
        return default if default
        raise KeyError, "key not found"
      end
      val
    ensure
      read_txn.abort if read_txn
    end

    def []=(key, data)
      @env.transaction do |txn|
        MDB.put(txn, @dbi, key, data)
      end
      data
    end

    def del(*args)
      @env.transaction do |txn|
        MDB.del(txn, @dbi, *args)
      end
      self
    end

    def each_key(key)
      raise ArgumentError, "no block given" unless block_given?
      read_txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(read_txn, @dbi)
      record = cursor.set_key(key)
      if record
        yield record
        while record = cursor.next_dup
          yield record
        end
      end
      self
    ensure
      cursor.close if cursor
      read_txn.abort if read_txn
    end

    def each
      raise ArgumentError, "no block given" unless block_given?
      read_txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(read_txn, @dbi)
      record = cursor.first
      if record
        yield record
        while record = cursor.next
          yield record
        end
      end
      self
    ensure
      cursor.close if cursor
      read_txn.abort if read_txn
    end

    def first
      read_txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(read_txn, @dbi)
      cursor.first
    ensure
      cursor.close if cursor
      read_txn.abort if read_txn
    end

    def last
      read_txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(read_txn, @dbi)
      cursor.last
    ensure
      cursor.close if cursor
      read_txn.abort if read_txn
    end

    def <<(value)
      txn = Txn.new(@env)
      cursor = Cursor.new(txn, @dbi)
      record = cursor.last
      if record
        cursor.put(record.first.to_fix.succ.to_bin, value, MDB::APPEND).close
      else
        cursor.put(0.to_bin, value, MDB::APPEND).close
      end
      txn.commit
      self
    rescue => e
      cursor.close if cursor
      txn.abort if txn
      raise e
    end

    def concat(values)
      txn = Txn.new(@env)
      cursor = Cursor.new(txn, @dbi)
      record = cursor.last
      key = 0
      if record
        key = record.first.to_fix.succ
      end
      values.each do |value|
        cursor.put(key.to_bin, value, MDB::APPEND)
        key += 1
      end
      cursor.close
      txn.commit
      self
    rescue => e
      cursor.close if cursor
      txn.abort if txn
      raise e
    end

    def stat
      read_txn = Txn.new(@env, RDONLY)
      MDB.stat(read_txn, @dbi)
    ensure
      read_txn.abort if read_txn
    end

    def length
      stat[:entries]
    end

    alias :size :length

    def empty?
      stat[:entries] == 0
    end

    def transaction(*args)
      raise ArgumentError, "no block given" unless block_given?
      txn = Txn.new(@env, *args)
      result = yield txn, @dbi
      txn.commit
      result
    rescue => e
      txn.abort if txn
      raise e
    end

    def cursor(*args)
      raise ArgumentError, "no block given" unless block_given?
      txn = Txn.new(@env, *args)
      cursor = Cursor.new(txn, @dbi)
      result = yield cursor
      cursor.close
      txn.commit
      result
    rescue => e
      cursor.close if cursor
      txn.abort if txn
      raise e
    end

    def to_a
      ary = []
      if flags & DUPSORT != 0
        each do |key, value|
          each_key(key) {|_key, _value| ary << [_key, _value]}
        end
      else
        each {|key, value| ary << [key, value]}
      end
      ary
    end

    def to_h
      hsh = {}
      if flags & DUPSORT != 0
        each do |key, value|
          each_key(key) do |key, value|
            hsh[key] ||= []
            hsh[key] << value
          end
        end
      else
        each {|key, value| hsh[key] = value}
      end
      hsh
    end
  end

  class Cursor
    Ops.keys.each do |m|
      define_method(m) do |key = nil, data = nil, static_string = false|
        get(Ops[m], key, data, static_string)
      end
    end
  end
end
