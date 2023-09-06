if Redis::VERSION < '4.2'
  module RedisCompatible
    extend ActiveSupport::Concern

    def exists?(key)
      exists(key)
    end
  end

  Redis.send(:include, RedisCompatible)
end

if defined?(Sidekiq::RedisClientAdapter::CompatClient)
  module RedisCompatible
    extend ActiveSupport::Concern

    def exists?(key)
      exists(key) != 0
    end
  end

  Sidekiq::RedisClientAdapter::CompatClient.include RedisCompatible
end