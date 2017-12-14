FILENAME = '/tmp/mtest.rocksdb'
OTHER_FILENAME = '/tmp/mtest_other.rocksdb'

assert 'RocksDB#open' do
  init = RocksDB.new(FILENAME)
  init.store('key1', 'value1')
  init.close

  db = RocksDB.new(FILENAME)
  assert_equal db.fetch('key1'), 'value1'
  db.close
end

assert 'RocksDB#clear' do
  db = RocksDB.new(FILENAME)
  db.store('key1', 'value1')
  db.store('key2', 'value2')
  db.store('key3', 'value3')

  db.clear

  assert_equal db.size, 0
end

assert 'RocksDB#closed' do
  db = RocksDB.new(FILENAME)
  db.close

  assert_true db.closed?
end

assert 'RocksDB#delete' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')
  db.delete('key1')

  assert_nil db.fetch('key1')
end

assert 'RocksDB#delete_if' do
  db = RocksDB.new(FILENAME)
  db.clear

  db.store('key1', 'value1')
  db.delete_if do |key, value|
    key == 'key1'
  end
  assert_nil db.fetch('key1')

  db.store('key1', 'value1')
  db.reject! do |key, value|
    key == 'key1'
  end
  assert_nil db.fetch('key1')
end

assert 'RocksDB#fetch_store' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')

  assert_equal db.fetch('key1'), 'value1'
  assert_nil db.fetch('key100')

  db['key2'] = 'value2'

  assert_equal db['key2'], 'value2'
  assert_nil db['key200']
end

assert 'RocksDB#each' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')
  db.store('key2', 'value2')
  db.store('key3', 'value3')

  db.each do |k, v|
    assert_true k == 'key1' || k == 'key2' || k == 'key3'
    assert_true v == 'value1' || v == 'value2' || v == 'value3'
  end

  db.each_pair do |k, v|
    assert_true k == 'key1' || k == 'key2' || k == 'key3'
    assert_true v == 'value1' || v == 'value2' || v == 'value3'
  end
end

assert 'RocksDB#each_key' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')
  db.store('key2', 'value2')
  db.store('key3', 'value3')

  db.each_key do |k|
    assert_true k == 'key1' || k == 'key2' || k == 'key3'
  end
end

assert 'RocksDB#each_value' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')
  db.store('key2', 'value2')
  db.store('key3', 'value3')

  db.each_value do |v|
    assert_true v == 'value1' || v == 'value2' || v == 'value3'
  end
end

assert 'RocksDB#has_key' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')

  assert_true db.has_key?('key1')
  assert_true db.key?('key1')
  assert_true db.include?('key1')
  assert_true db.member?('key1')

  db.delete('key1')

  assert_false db.has_key?('key1')
  assert_false db.key?('key1')
  assert_false db.include?('key1')
  assert_false db.member?('key1')
end

assert 'RocksDB#has_value' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')

  assert_true db.has_value?('value1')
  assert_true db.value?('value1')

  db.delete('key1')

  assert_false db.has_value?('value1')
  assert_false db.value?('value1')
end

assert 'RocksDB#keys' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')
  db.store('key2', 'value2')
  db.store('key3', 'value3')

  keys = db.keys
  assert_true keys.include? 'key1'
  assert_true keys.include? 'key2'
  assert_true keys.include? 'key3'
end

assert 'RocksDB#values' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')
  db.store('key2', 'value2')
  db.store('key3', 'value3')

  values = db.values
  assert_true values.include? 'value1'
  assert_true values.include? 'value2'
  assert_true values.include? 'value3'
end

assert 'RocksDB#invert' do
  db = RocksDB.new(FILENAME, 0666, RocksDB::NEWDB)
  db.clear
  db.store('key1', 'value1')
  db.store('key2', 'value2')
  db.store('key3', 'value3')

  hash = db.invert
  assert_true hash.is_a?(Hash)

  assert_true hash['value1'] == 'key1'
  assert_true hash['value2'] == 'key2'
  assert_true hash['value3'] == 'key3'
end

assert 'RocksDB#values_at' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')
  db.store('key2', 'value2')
  db.store('key3', 'value3')

  values = db.values_at('key1', 'key2')
  assert_true values.include? 'value1'
  assert_true values.include? 'value2'
  assert_false values.include? 'value3'
end

assert 'RocksDB#shift' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')

  pair1 = db.shift
  assert_equal pair1, ['key1', 'value1']
  assert_true db.empty?
end

#
# Implemented by Enumerable
#

assert 'RocksDB#to_a' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')
  db.store('key2', 'value2')
  db.store('key3', 'value3')

  array = db.to_a
  assert_true array.include? ['key1', 'value1']
  assert_true array.include? ['key2', 'value2']
  assert_true array.include? ['key3', 'value3']
end

assert 'RocksDB#to_hash' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')
  db.store('key2', 'value2')
  db.store('key3', 'value3')

  hash = db.to_hash
  assert_true hash.is_a?(Hash)

  assert_true hash['key1'] == 'value1'
  assert_true hash['key2'] == 'value2'
  assert_true hash['key3'] == 'value3'
end

assert 'RocksDB#select' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')
  db.store('key2', 'value2')
  db.store('key3', 'value3')

  selected = db.select do |key, value|
    key == 'key1'
  end

  assert_equal selected, [['key1', 'value1']]
end

assert 'RocksDB#reject' do
  db = RocksDB.new(FILENAME)
  db.clear

  db.store('key1', 'value1')
  db.store('key2', 'value2')
  db.store('key3', 'value3')

  rejected = db.reject do |key, value|
    key == 'key1'
  end

  assert_true rejected.is_a?(Hash)
  assert_false rejected.has_key?('key1')
  assert_true rejected.has_key?('key2')
  assert_true rejected.has_key?('key3')
end

assert 'RocksDB#replace' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')
  db.store('key2', 'value2')
  db.store('key3', 'value3')

  other = RocksDB.new(OTHER_FILENAME)
  other.clear
  other.store('other_key1', 'other_value1')
  other.store('other_key2', 'other_value2')
  other.store('other_key3', 'other_value3')

  db.replace(other)

  assert_false db.has_key?('key1')
  assert_false db.has_key?('key2')
  assert_false db.has_key?('key3')
  assert_true db.has_key?('other_key1')
  assert_true db.has_key?('other_key2')
  assert_true db.has_key?('other_key3')
end

assert 'RocksDB#update' do
  db = RocksDB.new(FILENAME)
  db.clear
  db.store('key1', 'value1')
  db.store('key2', 'value2')
  db.store('key3', 'value3')

  other = RocksDB.new(OTHER_FILENAME)
  other.clear
  other.store('key1', 'other_value1')
  other.store('other_key2', 'other_value2')
  other.store('other_key3', 'other_value3')

  db.update(other)

  assert_true db['key1'] = 'other_value1'
  assert_true db['key2'] = 'value2'
  assert_true db['key3'] = 'value3'
  assert_true db['other_key2'] = 'other_value2'
  assert_true db['other_key3'] = 'other_value3'
end
