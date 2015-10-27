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
          raise ArgumentError, "unknown option #{k}"
        end
      end
      instance
    end

    def transaction(*args)
      raise ArgumentError, "no block given" unless block_given?
      txn = Txn.new(self, *args)
      yield txn
      txn.commit
      self
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
      @env.transaction do |txn|
        @dbi = Dbi.open(txn, *args)
      end
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
      txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(txn, @dbi)
      while record = cursor.next_dup(key)
        yield record
      end
      self
    ensure
      cursor.close if cursor
      txn.abort if txn
    end

    def each
      raise ArgumentError, "no block given" unless block_given?
      txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(txn, @dbi)
      while record = cursor.next
         yield record
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
        key = record.first.to_fix + 1
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
      txn = Txn.new(@env, RDONLY)
      MDB.stat(txn, @dbi)
    ensure
      txn.abort if txn
    end

    def length
      stat[:entries]
    end

    alias :size :length

    def empty?
      length == 0
    end

    def transaction(*args)
      raise ArgumentError, "no block given" unless block_given?
      txn = Txn.new(@env, *args)
      yield txn, @dbi
      txn.commit
      self
    rescue => e
      txn.abort if txn
      raise e
    end

    def cursor(*args)
      raise ArgumentError, "no block given" unless block_given?
      txn = Txn.new(@env, *args)
      cursor = Cursor.new(txn, @dbi)
      yield cursor
      cursor.close
      txn.commit
      self
    rescue => e
      cursor.close if cursor
      txn.abort if txn
      raise e
    end
  end

  class Cursor
    [:first, :first_dup, :get_both, :get_both_range, :get_current, :get_multiple, :last, :last_dup, :next, :next_dup, :next_multiple,
      :next_nodup, :prev, :prev_dup, :prev_nodup, :set, :set_key, :set_range].each do |m|
      define_method(m) do |key = nil, data = nil, static_string = false|
        get("MDB::Cursor::#{m.to_s.upcase}".constantize, key, data, static_string)
      end
    end
  end
end
