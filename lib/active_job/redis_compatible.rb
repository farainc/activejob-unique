if defined?(Redis) && Redis::VERSION < '4.2'
  if defined?(Sidekiq) && Sidekiq::VERSION < '7'
    module RedisCompatible
      extend ActiveSupport::Concern

      def exists?(key)
        exists(key)
      end
    end

    Redis.send(:include, RedisCompatible)
  end
end