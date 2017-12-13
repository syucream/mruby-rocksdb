class RocksDB

  def clear
    self.each_key do |k|
      self.delete(k)
    end
  end

  def delete_if?
    self.each do |k, v|
      self.delete(k) if yield(k, v)
    end
  end

  def empty?
    self.count == 0
  end

  def has_value?(value)
    has_value = false

    self.each_value do |v|
      has_value = true if v == value
    end

    has_value
  end

  def to_hash
    hash = {}

    self.map do |k, v|
      hash[k] = v
    end

    hash
  end

  def keys
    self.map do |k, v|
      k
    end
  end

  def values
    self.map do |k, v|
      v
    end
  end

  def values_at(*keys)
    array = self.flat_map do |k, v|
      if keys.include?(k)
        [k]
      else
        []
      end
    end

    if array.empty?
      nil
    else
      array
    end
  end

  alias length count
  alias size count
  alias value? has_value?
end
