MRuby::Gem::Specification.new('mruby-rocksdb') do |spec|
  spec.license = 'MIT'
  spec.authors = 'Ryo Okubo'
  spec.version = '0.0.1'

  spec.cxx.flags << "-std=c++11"
  spec.linker.flags_after_libraries << "-lrocksdb"
end
