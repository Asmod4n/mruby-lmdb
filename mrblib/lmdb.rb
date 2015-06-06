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
      txn = Txn.new(@env, RDONLY)
      data = MDB.get(txn, @dbi, key)
      txn.abort
      data
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
      txn = Txn.new(@env, RDONLY)
      cursor = Cursor.new(txn, @dbi)
      record = cursor.get(Cursor::FIRST)
      if record
        yield record
        while record = cursor.get(Cursor::NEXT)
         yield record
        end
      end
      cursor.close
      txn.abort
      self
    end

    def stat
      txn = Txn.new(@env, RDONLY)
      stat = MDB.stat(txn, @dbi)
      txn.abort
      stat
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
