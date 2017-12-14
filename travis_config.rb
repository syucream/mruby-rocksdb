MRuby::Build.new do |conf|
  toolchain :gcc

  enable_debug
  conf.enable_bintest
  conf.enable_test

  # To test with rocksdb
  conf.linker.flags_after_libraries << "-lrocksdb"

  conf.gembox 'default'
  conf.gem './'
end
