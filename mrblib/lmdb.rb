module MDB
  Stat = Struct.new(:psize, :depth, :branch_pages, :leaf_pages, :overflow_pages, :entries)
  class Env
    Info = Struct.new(:mapaddr, :mapsize, :last_pgno, :last_txnid, :maxreaders, :numreaders)

    def transaction(*args)
      txn = nil
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

    def initialize(env, *args)
      @env = env
      @env.transaction do |txn|
        @dbi = Dbi.open(txn, *args)
      end
    end

    def [](key)
      txn = nil
      txn = Txn.new(@env, RDONLY)
      MDB.get(txn, @dbi, key)
    ensure
      txn.abort if txn
    end

    def []=(key, value)
      @env.transaction do |txn|
        MDB.put(txn, @dbi, key, value)
      end
      value
    end

    def del(*args)
      @env.transaction do |txn|
        MDB.del(txn, @dbi, *args)
      end
      self
    end

    def each
      txn = nil
      cursor = nil
      txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(txn, @dbi)
      while record = cursor.get(Cursor::NEXT)
         yield record
      end
      self
    ensure
      cursor.close if cursor
      txn.abort if txn
    end

    def first
      txn = nil
      cursor = nil
      txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(txn, @dbi)
      cursor.get(Cursor::FIRST)
    ensure
      cursor.close if cursor
      txn.abort if txn
    end

    def last
      txn = nil
      cursor = nil
      txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(txn, @dbi)
      cursor.get(Cursor::LAST)
    ensure
      cursor.close if cursor
      txn.abort if txn
    end

    def <<(value)
      txn = nil
      cursor = nil
      txn = Txn.new(@env)
      cursor = Cursor.new(txn, @dbi)
      record = cursor.get(Cursor::LAST, true)
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

    def stat
      txn = nil
      txn = Txn.new(@env, RDONLY)
      MDB.stat(txn, @dbi)
    ensure
      txn.abort if txn
    end

    def size
      stat[:entries]
    end

    def transaction(*args)
      @env.transaction(*args) do |txn|
        yield txn, @dbi
      end
      self
    end
  end
end
