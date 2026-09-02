MRuby::Gem::Specification.new('mruby-lmdb') do |spec|
  spec.license = 'Apache-2'
  spec.author  = 'Hendrik Beskow'
  spec.summary = 'mruby bindings for LMDB (Lightning Memory-Mapped Database)'
  spec.version = '2.0.0'

  spec.add_dependency 'mruby-errno'
  spec.add_dependency 'mruby-struct'
  spec.add_dependency 'mruby-c-ext-helpers'
  spec.add_test_dependency 'mruby-random'
  spec.add_test_dependency 'mruby-io'
  spec.add_test_dependency 'mruby-dir'
  spec.add_test_dependency 'mruby-string-ext'

  if spec.build.toolchains.include?('android')
    spec.cc.defines << 'HAVE_PTHREADS'
  elsif !spec.build.toolchains.include?('visualcpp')
    spec.linker.libraries << 'pthread'
  end

  # The error classes are DERIVED from lmdb.h, not maintained beside it.
  # LMDB is a pinned submodule: when it moves it brings new error codes
  # along, and a hand-written list goes stale in silence - the symptom is
  # an error arriving as the generic MDB::Error instead of its own class,
  # which surfaces only when somebody finally asks for the specific one.
  # Regenerating here means the list cannot disagree with the header this
  # build is actually compiling against.
  # UNCONDITIONALLY, every build. An mtime comparison looks cheaper but
  # is a trap: a fresh clone gives every file the same timestamp, so the
  # check would never fire on exactly the machine that has never
  # generated anything. It costs milliseconds.
  if File.exist?("#{spec.dir}/lmdb/libraries/liblmdb/lmdb.h")
    sh "ruby #{spec.dir}/src/gen_errors.rb"
  end

  lmdb_src = "#{spec.dir}/lmdb/libraries/liblmdb"
  spec.cc.include_paths << lmdb_src
  spec.objs += %W(
    #{lmdb_src}/mdb.c
    #{lmdb_src}/midl.c
  ).map { |f| f.relative_path_from(dir).pathmap("#{build_dir}/%X#{spec.exts.object}") }
end
