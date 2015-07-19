# mruby-lmdb
mruby wrapper for Lightning Memory-Mapped Database from Symas http://symas.com/mdb/

Prerequisites
=============
lmdb needs to be somewhere your compiler can find it.

Examples
========

```ruby
env = MDB::Env.new
env.open('testdb', MDB::NOSUBDIR)
db = env.database

db << "hallo0" << "hallo1" << "hallo2"

db[18.to_bin] = "hallo18"

db.each do |k, v|
  puts "#{k.to_fix} = #{v}"
end

puts db.first
puts db.last

puts db.stat
puts env.info

```
