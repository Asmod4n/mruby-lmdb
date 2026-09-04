#!/usr/bin/env ruby

# The error list is DERIVED, never maintained. LMDB is a pinned submodule;
# when it moves, it brings new codes with it, and a hand-written list goes
# stale silently - the symptom is an error arriving as the generic
# MDB::Error instead of its own class, which nobody notices until a test
# asks for the specific one.
#
# So the names come out of lmdb.h itself. Every LMDB error is a #define in
# the -30xxx block; MDB_SUCCESS is 0 and MDB_LAST_ERRCODE is an alias, so
# neither matches and neither has to be excluded by name.

Dir.chdir(File.dirname($0))

HEADER = '../lmdb/libraries/liblmdb/lmdb.h'

unless File.exist?(HEADER)
  abort "#{HEADER} is missing - the lmdb submodule is not checked out " \
        "(git submodule update --init)"
end

names = []
IO.readlines(HEADER).each do |line|
  # (-30799) through (-30769) today, and whatever the block grows to.
  next unless line =~ /^#define\s+MDB_([A-Z0-9_]+)\s+\(-30\d{3}\)/
  names << $1
end

abort "found no error codes in #{HEADER} - has the header changed shape?" if names.empty?

# Written only when the bytes differ. mrbgem.rake runs this script on
# every `rake compile`, and an unconditional write gives both files a
# new mtime even when the list is the same. src/mrb_lmdb.c includes the
# cstub, so mrb_lmdb.o recompiled on every single run, on a tree where
# nothing had changed. Measured from webmachine-mruby, which carries
# this gem: one object per idle build.
#
# The comparison is on CONTENT, so the derivation still holds. When the
# lmdb submodule moves and brings new codes, the bytes differ, the file
# is written, and the rebuild follows - which is the whole reason this
# list is derived and never maintained.
def write_if_changed(path, content)
  return if File.exist?(path) && File.binread(path) == content
  File.binwrite(path, content)
end

write_if_changed('known_errors_def.cstub',
                 names.map { |name| %(mrb_lmdb_define_error(MDB_#{name}, "#{name}");\n) }.join)

# Kept for readability and for anyone diffing what the header offered.
write_if_changed('known_errors.def',
                 "# GENERATED from lmdb.h by gen_errors.rb - do not edit by hand.\n" +
                 names.map { |name| "#{name}\n" }.join)

puts "#{names.size} error codes from #{HEADER}"
