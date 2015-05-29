module MDB
  class Env
    def transaction
      txn = nil
      txn = Txn.new self
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
      @env.transaction do |txn|
        data = MDB.get(txn, @dbi, key)
      end
      data
    end

    def []=(key, value)
      ret = nil
      @env.transaction do |txn|
        ret = MDB.put(txn, @dbi, key, value)
      end
      ret
    end
  end
end
