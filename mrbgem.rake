MRuby::Gem::Specification.new('mruby-lmdb') do |spec|
  spec.license = 'Apache-2'
  spec.author  = 'Hendrik Beskow'
  spec.summary = 'mruby bindings for lmdb'
  spec.add_dependency 'mruby-errno'
  spec.add_dependency 'mruby-struct'

  if build.toolchains.include?('android')
    spec.cc.defines << 'HAVE_PTHREADS'
  else
    spec.linker.libraries << 'pthread'
  end
end
