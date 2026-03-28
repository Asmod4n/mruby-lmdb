unless Object.const_defined?(:IOError)
  class IOError < StandardError; end
end

module MDB
  class Stat < Struct.new(:psize, :depth, :branch_pages, :leaf_pages, :overflow_pages, :entries); end

  class Env
    class Info < Struct.new(:mapaddr, :mapsize, :last_pgno, :last_txnid, :maxreaders, :numreaders); end
  end

  class Database
    include Enumerable
  end

  class Cursor
    Ops.keys.each do |m|
      define_method(m) do |key = nil, data = nil|
        get(Ops[m], key, data)
      end
    end
  end
end
