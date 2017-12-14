MRuby::Build.new do |conf|
  toolchain :gcc

  enable_debug
  conf.gembox 'default'
  conf.enable_bintest
  conf.enable_test
  conf.enable_cxx_abi

  # To test with rocksdb
  conf.linker.flags_after_libraries << "-lrocksdb"

  conf.gem './'
end
