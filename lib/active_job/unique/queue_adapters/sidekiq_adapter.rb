require 'active_support/concern'
require 'sidekiq/api'
require 'sidekiq/worker'

module ActiveJob
  module Unique
    module QueueAdapters
      module SidekiqAdapter
        extend ActiveSupport::Concern

        module ClassMethods
          def progress_stats_initialize(sets, job_name)
            Sidekiq.redis_pool.with do |conn|
              conn.sadd(sets, job_name)
            end
          end

          def getset_progress_state(state_key, data)
            Sidekiq.redis_pool.with do |conn|
              conn.getset(state_key, data)
            end
          end

          def get_progress_state(state_key)
            Sidekiq.redis_pool.with do |conn|
              conn.get(state_key)
            end
          end

          def set_progress_state(state_key, data)
            Sidekiq.redis_pool.with do |conn|
              conn.set(state_key, data)
            end
          end

          def expire_progress_state(state_key, seconds)
            Sidekiq.redis_pool.with do |conn|
              conn.expire(state_key, seconds)
            end
          end

          def incr_progress_stats(stats_group, stats_key, day)
            Sidekiq.redis_pool.with do |conn|
              conn.multi do
                # total
                conn.hsetnx(stats_group, stats_key, 0)
                conn.hincrby(stats_group, stats_key, 1)

                # day
                conn.hsetnx("#{stats_group}:#{day}", stats_key, 0)
                conn.hincrby("#{stats_group}:#{day}", stats_key, 1)
              end
            end
          end

          def another_job_in_queue?(uniqueness_id, queue_name)
            queue = Sidekiq::Queue.new(queue_name)
            return false if queue.size.zero?

            queue.any? { |job| job.item['args'][0]['uniqueness_id'] == uniqueness_id }
          end

          def another_job_in_worker?(uniqueness_id, queue_name, job_id)
            Sidekiq::Workers.new.any? { |_p, _t, w| w['queue'] == queue_name && w['payload']['args'][0]['uniqueness_id'] == uniqueness_id && w['payload']['args'][0]['job_id'] != job_id }
          end

          # support rails 4.2x (add provider_job_id to job_data)
          # def enqueue(job)
          #   # Sidekiq::Client does not support symbols as keys
          #   job.provider_job_id = Sidekiq::Client.push \
          #     "class"   => JobWrapper,
          #     "wrapped" => job.class.to_s,
          #     "queue"   => job.queue_name,
          #     "args"    => [ job.serialize ]
          # end
          #
          # def enqueue_at(job, timestamp)
          #   job.provider_job_id = Sidekiq::Client.push \
          #     "class"   => JobWrapper,
          #     "wrapped" => job.class.to_s,
          #     "queue"   => job.queue_name,
          #     "args"    => [ job.serialize ],
          #     "at"      => timestamp
          # end
        end

        # class JobWrapper
        #   include Sidekiq::Worker
        #
        #   def perform(job_data)
        #     Base.execute job_data.merge("provider_job_id" => jid)
        #   end
        # end

        def progress_stats_initialize(*args)
          self.class.progress_stats_initialize(*args)
        end

        def incr_progress_stats(*args)
          self.class.incr_progress_stats(*args)
        end

        def getset_progress_state(*args)
          self.class.getset_progress_state(*args)
        end

        def get_progress_state(*args)
          self.class.get_progress_state(*args)
        end

        def set_progress_state(*args)
          self.class.set_progress_state(*args)
        end

        def expire_progress_state(*args)
          self.class.expire_progress_state(*args)
        end

        def another_job_in_queue?(*args)
          self.class.another_job_in_queue?(*args)
        end

        def another_job_in_worker?(*args)
          self.class.another_job_in_worker?(*args)
        end
        #
        # def dirty_uniqueness?(*args)
        #   self.class.dirty_uniqueness?(*args)
        # end
        #
        # def read_uniqueness(*args)
        #   self.class.read_uniqueness(*args)
        # end
        #
        # def write_progress(*args)
        #   self.class.write_progress(*args)
        # end
        #
        # def update_progress(*args)
        #   self.class.update_progress(*args)
        # end
        #
        # def expire_uniqueness(*args)
        #   self.class.expire_uniqueness(*args)
        # end
        #
        # def clean_uniqueness(*args)
        #   self.class.clean_uniqueness(*args)
        # end
        #
        # def cleanup_uniqueness_timeout(*args)
        #   self.class.cleanup_uniqueness_timeout(*args)
        # end
        #
        # def cleanup_uniqueness_all(*args)
        #   self.class.cleanup_uniqueness_all(*args)
        # end
        #
        # def incr_job_stats(*args)
        #   self.class.incr_job_stats(*args)
        # end
        #
        # def sync_overall_stats(*args)
        #   self.class.sync_overall_stats(*args)
        # end

      end
    end
  end
end

ActiveJob::QueueAdapters::SidekiqAdapter.send(:include, ActiveJob::Unique::QueueAdapters::SidekiqAdapter)
