# mruby-lmdb
mruby wrapper for Lightning Memory-Mapped Database from Symas http://symas.com/mdb/

Prerequisites
=============
lmdb needs to be somewhere your compiler can find it.

On Ubuntu:
```sh
apt install liblmdb-dev
```

On OS X:
```sh
brew install lmdb
```

Examples
========

```ruby
env = MDB::Env.new
env.open('testdb', MDB::NOSUBDIR)
db = env.database(MDB::INTEGERKEY)

db << 'hallo0' << 'hallo1' << 'hallo2'

db[18.to_bin] = 'hallo18'

db.concat ['hallo19', 'hallo20'] #for faster batch import

db.each do |k, v|
  puts "#{k.to_fix} = #{v}"
end

db.transaction(MDB::RDONLY) do |txn, dbi|
  cursor = MDB::Cursor.new(txn, dbi)
  puts cursor.set_range(10.to_bin)
end

puts db.first
puts db.last

puts db.stat
puts env.info
puts env.stat

```
