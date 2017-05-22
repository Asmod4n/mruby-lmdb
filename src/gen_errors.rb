#!/usr/bin/env ruby

Dir.chdir(File.dirname($0))

d = File.open("known_errors_def.cstub", "w")

IO.readlines("known_errors.def").each { |name|
  next if name =~ /^#/
  name.strip!

  d.write <<-C
#ifdef MDB_#{name}
mrb_lmdb_define_error(MDB_#{name}, "#{name}");
#endif
C
}
