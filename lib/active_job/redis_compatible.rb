if Redis::VERSION < '4.2'
  module RedisCompatible
    extend ActiveSupport::Concern

    def exists?(key)
      exists(key)
    end
  end

  Redis.send(:include, RedisCompatible)
end