if defined?(Redis)
  if Redis::VERSION < '4.2'
    module RedisCompatible
      extend ActiveSupport::Concern

      def exists?(key)
        exists(key)
      end
    end

    Redis.send(:include, RedisCompatible)
  elsif Redis::VERSION > '4.2' && Redis::VERSION < '5'
    Redis.exists_returns_integer = false
  elsif Redis::VERSION >= '5'
    module RedisCompatible
      extend ActiveSupport::Concern

      def exists?(key)
        exists(key) != 0
      end
    end

    Redis.send(:include, RedisCompatible)
  end
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