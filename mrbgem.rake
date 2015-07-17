MRuby::Gem::Specification.new('mruby-lmdb') do |spec|
  spec.license = 'Apache-2'
  spec.author  = 'Hendrik Beskow'
  spec.summary = 'mruby bindings to lmdb'
  spec.linker.libraries << 'lmdb'
  spec.add_dependency 'mruby-errno'
end
