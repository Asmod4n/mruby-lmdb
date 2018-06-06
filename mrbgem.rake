MRuby::Gem::Specification.new('mruby-lmdb') do |spec|
  spec.license = 'Apache-2'
  spec.author  = 'Hendrik'
  spec.summary = 'mruby bindings for lmdb'
  spec.add_dependency 'mruby-errno'
  spec.add_dependency 'mruby-struct'

  if spec.build.toolchains.include?('android')
    spec.cc.defines << 'HAVE_PTHREADS'
  elsif !spec.build.toolchains.include?('visualcpp')
    spec.linker.libraries << 'pthread'
  end

  if spec.cc.search_header_path('lmdb.h')
    spec.linker.libraries << 'lmdb'
  else
    lmdb_src = "#{spec.dir}/lmdb/libraries/liblmdb"
    spec.cc.include_paths << "#{lmdb_src}"
    spec.objs += %W(
      #{lmdb_src}/mdb.c
      #{lmdb_src}/midl.c
    ).map { |f| f.relative_path_from(dir).pathmap("#{build_dir}/%X#{spec.exts.object}" ) }
  end
end
