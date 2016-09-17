require 'sidekiq'
require_relative 'sidekiq_extension/web'

module ActiveJob
  module JobStats
    module SidekiqExtension
      def self.sequence_today
        Time.now.utc.to_date.strftime('%Y%m%d').to_i
      end

      def self.cleanup_hash_set(key)
        newkey = "#{key}:#{SecureRandom.hex}"
        counter = 0

        Sidekiq.redis_pool.with do |conn|
          return counter if conn.hlen(key).zero?

          conn.rename(key, newkey)

          cursor = '0'
          loop do
            cursor, fields = conn.hscan(newkey, cursor, count: 100)
            hkeys = fields.map { |pair| pair[0] }
            conn.hdel(newkey, hkeys) unless hkeys.empty?
            counter += hkeys.size
            break if cursor == '0'
          end
        end

        counter
      end
    end
  end
end
