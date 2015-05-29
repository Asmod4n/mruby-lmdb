module MDB
  class Env
    def self.new(*args)
      instance = super()
      instance.open(*args)
      instance
    end

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
      dbi = nil
      transaction do |txn|
        dbi = Dbi.open(txn, *args)
      end
      dbi
    end
  end

  class Database
    def initialize(env, *args)
      @env = env
      @dbi = @env.database(*args)
    end

    def [](key)
      data = nil
      @env.transaction(MDB::RDONLY) do |txn|
        data = MDB.get(txn, @dbi, key)
      end
      data
    end

    def []=(key, value)
      @env.transaction do |txn|
        MDB.put(txn, @dbi, key, value)
      end
      self
    end

    def del(*args)
      @env.transaction do |txn|
        MDB.del(txn, @dbi, *args)
      end
      self
    end

    def transaction(*args)
      @env.transaction(*args) do |txn|
        yield txn, @dbi
      end
      self
    end
  end
end
