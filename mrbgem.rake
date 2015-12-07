MRuby::Gem::Specification.new('mruby-lmdb') do |spec|
  spec.license = 'Apache-2'
  spec.author  = 'Hendrik Beskow'
  spec.summary = 'mruby bindings to lmdb'
  spec.add_dependency 'mruby-errno'

  if build.toolchains.include?('android')
    spec.cc.flags << '-DHAVE_PTHREADS'
  else
    spec.linker.libraries << 'pthread'
  end
end
