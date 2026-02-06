if defined?(Redis)
  module ZaddCompatible
    extend ActiveSupport::Concern

    def zadd_safe(*args, **kwargs)
      zadd(*args, **kwargs)
    end
  end

  Redis.send(:include, ZaddCompatible)

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

  if Sidekiq::VERSION >= '7.1.0'
    module SidekiqRedisHelpers
      extend ActiveSupport::Concern

      def getset(key, value)
        args = [key, value, "GET"]

        @client.call("SET", *args)
      end

      def zadd_safe(key, score_and_member, **kwargs)
        args = [key]

        args << "NX"  if kwargs[:nx]
        args << "XX"  if kwargs[:xx]
        args << "CH"  if kwargs[:ch]
        args << "INCR" if kwargs[:incr]

        args += score_and_member

        @client.call("ZADD", *args)
      end

      def zrevrange(key, start, stop)
        args = [key, start, stop, "REV"]

        @client.call("ZRANGE", *args)
      end

      def zrangebyscore(key, min, max, limit: nil)
        args = [key, min, max, "BYSCORE"]

        unless limit.nil?
          args << "LIMIT"
          args += limit
        end

        @client.call("ZRANGE", *args)
      end

      def zrevrangebyscore(key, max, min, limit: nil)
        args = [key, min, max, "BYSCORE", "REV"]

        unless limit.nil?
          args << "LIMIT"
          args += limit
        end

        @client.call("ZRANGE", *args)
      end

      def scan_each(match: nil, count: nil, &block)
        cursor = "0"
        loop do
          args = [cursor]
          args += ["MATCH", match] if match
          args += ["COUNT", count] if count
          cursor, keys = @client.call("SCAN", *args)
          keys.each { |key| block.call(key) }
          break if cursor == "0"
        end
      end

      def hscan_each(key, match: nil, count: nil, &block)
        cursor = "0"
        loop do
          args = [key, cursor]
          args += ["MATCH", match] if match
          args += ["COUNT", count] if count
          cursor, entries = @client.call("HSCAN", *args)
          entries.each_slice(2) do |field, value|
            block.call(field, value)
          end
          break if cursor == "0"
        end
      end
    end

    Sidekiq::RedisClientAdapter::CompatClient.include SidekiqRedisHelpers
  end
end