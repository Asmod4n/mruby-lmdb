MRuby::Gem::Specification.new('mruby-lmdb') do |spec|
  spec.license = 'Apache-2'
  spec.author  = 'Hendrik Beskow'
  spec.summary = 'mruby bindings to lmdb'
  spec.add_dependency 'mruby-errno'
  spec.linker.libraries << 'lmdb'
end
