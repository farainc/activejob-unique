require 'active_support/concern'
require 'sidekiq/api'

module ActiveJob
  module Unique
    module QueueAdapters
      module SidekiqAdapter
        extend ActiveSupport::Concern

        module ClassMethods
          DATA_SEPARATOR = 0x1E.chr

          def sequence_today
            Time.now.utc.to_date.strftime('%Y%m%d').to_i
          end

          def dirty_uniqueness?(queue_name, scheduled)
            empty_queue = Sidekiq::Queue.new(queue_name).size.zero?
            empty_worker = Sidekiq::Workers.new.size.zero?
            empty_scheduled = !scheduled || Sidekiq::ScheduledSet.new.size.zero?

            empty_queue && empty_worker && empty_scheduled
          end

          def read_uniqueness(uniqueness_id, queue_name)
            uniqueness = nil

            Sidekiq.redis_pool.with do |conn|
              uniqueness = conn.hget("uniqueness:#{queue_name}", uniqueness_id)
            end

            uniqueness
          end

          def write_uniqueness_dump(uniqueness_id, queue_name, klass, args, job_id, uniqueness_mode, timeout = 0, scheduled = false)
            return if klass.blank?

            Sidekiq.redis_pool.with do |conn|
              conn.hset("uniqueness:dump:#{queue_name}", uniqueness_id, [klass, job_id, uniqueness_mode, timeout, scheduled, args].join(DATA_SEPARATOR).to_s.force_encoding('UTF-8'))
            end
          end

          def write_uniqueness_progress(uniqueness_id, queue_name, progress, timeout = 0, scheduled = false)
            expires = 30.minutes.from_now.utc.to_i

            Sidekiq.redis_pool.with do |conn|
              conn.hset("uniqueness:#{queue_name}", uniqueness_id, [timeout, scheduled, progress, expires].join(DATA_SEPARATOR).to_s)
            end
          end

          def clean_uniqueness(uniqueness_id, queue_name)
            Sidekiq.redis_pool.with do |conn|
              conn.multi do
                conn.hdel("uniqueness:#{queue_name}", uniqueness_id)
                conn.hdel("uniqueness:dump:#{queue_name}", uniqueness_id)
              end
            end
          end

          def cleanup_uniqueness_timeout(limit = 10000)
            queue_names = Sidekiq::Queue.all.map(&:name)
            output = {}

            Sidekiq.redis_pool.with do |conn|
              queue_names.each do |name|
                next unless (name =~ /^#{ActiveJob::Base.queue_name_prefix}/i).present?
                output[name] = 0
                cursor = '0'

                loop do
                  cursor, fields = conn.hscan("uniqueness:#{name}", cursor, count: 100)

                  fields.each do |uniqueness_id, uniqueness|
                    data = uniqueness.split(DATA_SEPARATOR)

                    # timeout, scheduled, progress, expires
                    timeout, _scheduled, _progress, expires = data
                    timeout = timeout.to_i
                    expires = expires.to_i

                    # expires only without timeout set
                    should_clean_it = timeout.blank? && expires.present? && Time.now.utc.to_i > expires
                    should_clean_it ||= timeout.present? && Time.now.utc.to_i > timeout

                    next unless should_clean_it

                    clean_uniqueness(uniqueness_id, name)
                    output[name] += 1
                  end

                  break if cursor == '0'
                  break if output[name] >= limit
                end
              end
            end

            output
          end

          def incr_job_stats(queue_name, klass, progress)
            Sidekiq.redis_pool.with do |conn|
              conn.multi do
                conn.hsetnx("jobstats:#{sequence_today}:#{progress}:#{queue_name}", klass, 0)
                conn.hincrby("jobstats:#{sequence_today}:#{progress}:#{queue_name}", klass, 1)
              end
            end
          end

          def sync_overall_stats(range = 1)
            today = sequence_today
            to = today - 1
            from = to - range

            queue_names = Sidekiq::Queue.all.map(&:name)
            output = {}

            Sidekiq.redis_pool.with do |conn|
              queue_names.each do |name|
                next unless (name =~ /^#{ActiveJob::Base.queue_name_prefix}/i).present?
                output[name] = 0

                (from..to).each do |day|
                  [:enqueue, :perform].each do |stage|
                    klasses = conn.hkeys("jobstats:#{day}:#{stage}_attempted:#{name}")

                    klasses.each do |klass|
                      [:attempted, :skiped, :processing, :failed, :processed].each do |progress|
                        val = conn.hget("jobstats:#{day}:#{stage}_#{progress}:#{name}", klass).to_i
                        if val.positive?
                          conn.hsetnx("jobstats:#{stage}_#{progress}:#{name}", klass, 0)
                          conn.hincrby("jobstats:#{stage}_#{progress}:#{name}", klass, val)
                        end

                        conn.hdel("jobstats:#{day}:#{stage}_#{progress}:#{name}", klass)
                      end
                    end
                  end
                end
              end
            end
          end
        end

        def sequence_today
          self.class.sequence_today
        end

        def dirty_uniqueness?(*args)
          self.class.dirty_uniqueness?(*args)
        end

        def read_uniqueness(*args)
          self.class.read_uniqueness(*args)
        end

        def write_uniqueness_dump(*args)
          self.class.write_uniqueness_dump(*args)
        end

        def write_uniqueness_progress(*args)
          self.class.write_uniqueness_progress(*args)
        end

        def clean_uniqueness(*args)
          self.class.clean_uniqueness(*args)
        end

        def cleanup_uniqueness_timeout(*args)
          self.class.cleanup_uniqueness_timeout(*args)
        end

        def incr_job_stats(*args)
          self.class.incr_job_stats(*args)
        end

        def sync_overall_stats(*args)
          self.class.sync_overall_stats(*args)
        end
      end
    end
  end
end

ActiveJob::QueueAdapters::SidekiqAdapter.send(:include, ActiveJob::Unique::QueueAdapters::SidekiqAdapter)
