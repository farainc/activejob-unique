require 'active_support/concern'
require 'sidekiq/api'

module ActiveJob
  module Unique
    module QueueAdapters
      module SidekiqAdapter
        extend ActiveSupport::Concern

        module ClassMethods
          DATA_SEPARATOR = 0x1E.chr

          def perform_stage?(progress)
            [:perform_processing, :perform_processed].inclde?(progress.to_sym)
          end

          def sequence_today
            Time.now.utc.to_date.strftime('%Y%m%d').to_i
          end

          def dirty_uniqueness?(queue_name, provider_job_id)
            job = Sidekiq::Queue.new(queue_name).find_job(provider_job_id)

            job.nil?
          end

          def read_uniqueness(uniqueness_id, queue_name)
            uniqueness = nil

            Sidekiq.redis_pool.with do |conn|
              uniqueness = conn.hget("uniqueness:#{queue_name}", uniqueness_id)
            end

            uniqueness
          end

          def write_uniqueness_dump(uniqueness_id, queue_name, klass, args, job_id, uniqueness_mode, expires)
            return if klass.blank?

            expires = 1.hour.from_now.to_i if expires < Time.now.utc.to_i

            Sidekiq.redis_pool.with do |conn|
              conn.hset("uniqueness:dump:#{queue_name}", uniqueness_id, [klass, job_id, uniqueness_mode, expires, expires + 2.hours, args].join(DATA_SEPARATOR).to_s.force_encoding('UTF-8'))
            end
          end

          def write_uniqueness_progress(uniqueness_id, queue_name, progress, expires)
            expires = 1.hour.from_now.to_i if expires < Time.now.utc.to_i

            Sidekiq.redis_pool.with do |conn|
              conn.hset("uniqueness:#{queue_name}", uniqueness_id, [progress, expires, expires + 2.hours].join(DATA_SEPARATOR).to_s)
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

          def cleanup_uniqueness_timeout(limit = 1000)
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
                    now = Time.now.utc.to_i
                    data = uniqueness.split(DATA_SEPARATOR)

                    # progress, expires, defaults
                    progress, expires, defaults = data
                    defaults = defaults.to_i
                    expires = expires.to_i

                    # expires when default expiration passed
                    should_clean_it = defaults.positive? && defaults < now
                    should_clean_it ||= perform_stage?(progress) && expires.positive? && expires < now

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

          def cleanup_uniqueness_all(limit = 10000)
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

        def perform_stage?(*args)
          self.class.perform_stage?(*args)
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

        def cleanup_uniqueness_all(*args)
          self.class.cleanup_uniqueness_all(*args)
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
