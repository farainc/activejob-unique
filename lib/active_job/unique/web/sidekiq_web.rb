require 'sidekiq'
require_relative 'sidekiq/server'

module ActiveJob
  module Unique
    module Web
      module SidekiqWeb
        extend ActiveSupport::Autoload

        def self.sequence_today
          Time.now.utc.to_date.strftime('%Y%m%d').to_i
        end

        def self.progress_stats_jobs
          "progress_stats"
        end

        def self.progress_stats_job_group(job_name)
          "#{progress_stats_jobs}:stats:#{job_name}"
        end

        def self.daily_progress_stats_job_group(job_name, day)
          "#{progress_stats_job_group(job_name)}:#{day}"
        end

        def self.progress_stats_job_group_key(queue_name, progress_stage)
          "#{queue_name}:#{progress_stage}"
        end

        def self.regroup_progress_stats_job_group(key_values)
          stats_job_group = {}

          key_values.each do |key, value|
            key_pair = key.to_s.split(":")

            queue_name = key_pair[0]
            stage = key_pair[1].split('_')

            unless stats_job_group.has_key?(queue_name)
              stats_job_group[queue_name] = {}
              stats_job_group[queue_name]['enqueue'] = {}
              stats_job_group[queue_name]['perform'] = {}
            end

            stats_job_group[queue_name][stage[0]][stage[1]] = value
          end

          stats_job_group
        end

        def self.progress_stats_job_group_search
          "#{progress_stats_jobs}:stats"
        end

        def self.progress_state_job_key(job_name, uniqueness_id, queue_name, stage)
          "#{progress_stats_jobs}:state:#{job_name}:#{uniqueness_id}:#{queue_name}:#{stage}"
        end

        def self.progress_state_job_search
          "#{progress_stats_jobs}:state"
        end

        # def self.cleanup_hash_set(key)
        #   newkey = "#{key}:#{SecureRandom.hex}"
        #   counter = 0
        #
        #   Sidekiq.redis_pool.with do |conn|
        #     return counter if conn.hlen(key).zero?
        #
        #     conn.rename(key, newkey)
        #
        #     cursor = '0'
        #     loop do
        #       cursor, fields = conn.hscan(newkey, cursor, count: 100)
        #       hkeys = fields.map { |pair| pair[0] }
        #       conn.hdel(newkey, hkeys) unless hkeys.empty?
        #       counter += hkeys.size
        #       break if cursor == '0'
        #     end
        #   end
        #
        #   counter
        # end

        autoload :Server
      end
    end
  end
end
