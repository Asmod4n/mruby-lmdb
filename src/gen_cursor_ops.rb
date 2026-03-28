#!/usr/bin/env ruby

Dir.chdir(File.dirname($0))

d = File.open("known_cursor_ops_def.cstub", "w")

IO.readlines("known_cursor_ops.def").each { |name|
  next if name =~ /^#/
  name.strip!

  d.write <<-C
mrb_lmdb_define_cursor_op(MDB_#{name}, "#{name.downcase}");
C
}
