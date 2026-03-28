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

  if spec.build.toolchains.include?('android')
    spec.cc.defines << 'HAVE_PTHREADS'
  elsif !spec.build.toolchains.include?('visualcpp')
    spec.linker.libraries << 'pthread'
  end

  lmdb_src = "#{spec.dir}/lmdb/libraries/liblmdb"
  spec.cc.include_paths << lmdb_src
  spec.objs += %W(
    #{lmdb_src}/mdb.c
    #{lmdb_src}/midl.c
  ).map { |f| f.relative_path_from(dir).pathmap("#{build_dir}/%X#{spec.exts.object}") }
end
