require 'active_support/concern'
require 'sidekiq/api'

module ActiveJob
  module Unique
    module QueueAdapters
      module SidekiqAdapter
        extend ActiveSupport::Concern

        module ClassMethods
          DATA_SEPARATOR = 0x1E.chr.freeze

          JOB_PROGRESS_ENQUEUE_ATTEMPTED = :enqueue_attempted
          JOB_PROGRESS_ENQUEUE_PROCESSING = :enqueue_processing
          JOB_PROGRESS_ENQUEUE_FAILED = :enqueue_failed
          JOB_PROGRESS_ENQUEUE_PROCESSED = :enqueue_processed
          JOB_PROGRESS_ENQUEUE_SKIPPED = :enqueue_skipped

          JOB_PROGRESS_PERFORM_ATTEMPTED = :perform_attempted
          JOB_PROGRESS_PERFORM_PROCESSING = :perform_processing
          JOB_PROGRESS_PERFORM_FAILED = :perform_failed
          JOB_PROGRESS_PERFORM_PROCESSED = :perform_processed
          JOB_PROGRESS_PERFORM_SKIPPED = :perform_skipped

          def ensure_data_utf8(data)
            data.to_s.encode('utf-8', invalid: :replace, undef: :replace, replace: '')
          end

          def sequence_today
            Time.now.utc.to_date.strftime('%Y%m%d').to_i
          end

          def enqueue_stage?(progress)
            [JOB_PROGRESS_ENQUEUE_ATTEMPTED,
             JOB_PROGRESS_ENQUEUE_PROCESSING,
             JOB_PROGRESS_ENQUEUE_PROCESSED,
             JOB_PROGRESS_ENQUEUE_FAILED,
             JOB_PROGRESS_ENQUEUE_SKIPPED].include?(progress.to_s.to_sym)
          end

          def perform_stage?(progress)
            [JOB_PROGRESS_PERFORM_ATTEMPTED,
             JOB_PROGRESS_PERFORM_PROCESSING,
             JOB_PROGRESS_PERFORM_PROCESSED,
             JOB_PROGRESS_PERFORM_FAILED,
             JOB_PROGRESS_PERFORM_SKIPPED].include?(progress.to_s.to_sym)
          end

          def unknown_stage?(progress)
            !enqueue_stage?(progress) && !perform_stage?(progress)
          end

          def enqueue_stage_job?(uniqueness_id, queue_name)
            uniqueness = read_uniqueness(uniqueness_id, queue_name)
            return false if uniqueness.blank?

            data = ensure_data_utf8(uniqueness).split(DATA_SEPARATOR)
            progress, timeout, expires = data

            enqueue_stage?(progress)
          end

          def perform_stage_job?(uniqueness_id, queue_name)
            uniqueness = read_uniqueness(uniqueness_id, queue_name)
            return false if uniqueness.blank?

            data = ensure_data_utf8(uniqueness).split(DATA_SEPARATOR)
            progress, timeout, expires = data

            perform_stage?(progress)
          end

          def dirty_uniqueness?(uniqueness)
            return true if uniqueness.blank?

            now = Time.now.utc.to_i
            data = ensure_data_utf8(uniqueness).split(DATA_SEPARATOR)

            # progress, timeout, expires
            progress, timeout, expires = data
            expires = expires.to_i
            timeout = timeout.to_i

            # allow when default expiration passed
            return true if expires.positive? && expires < now

            # allow when perform stage and expiration passed
            return true if perform_stage?(progress) && timeout.positive? && timeout < now

            # allow unknown stage
            return true if unknown_stage?(progress)

            false
          end

          def read_uniqueness(uniqueness_id, queue_name)
            uniqueness = nil

            Sidekiq.redis_pool.with do |conn|
              uniqueness = conn.hget("uniqueness:#{queue_name}", uniqueness_id)
            end

            uniqueness
          end

          def read_uniqueness_dump(uniqueness_id, queue_name)
            uniqueness = nil

            Sidekiq.redis_pool.with do |conn|
              uniqueness = conn.hget("uniqueness:dump:#{queue_name}", uniqueness_id)
            end

            uniqueness
          end

          def write_uniqueness_progress(uniqueness_id, queue_name, progress, timeout, expires)
            Sidekiq.redis_pool.with do |conn|
              conn.hset("uniqueness:#{queue_name}", uniqueness_id, ensure_data_utf8([progress, timeout, expires, Time.now.utc.to_i].join(DATA_SEPARATOR)))
            end
          end

          def write_uniqueness_dump(uniqueness_id, queue_name, klass, args, job_id, uniqueness_mode)
            return if klass.blank?

            Sidekiq.redis_pool.with do |conn|
              conn.hset("uniqueness:dump:#{queue_name}", uniqueness_id, ensure_data_utf8([klass, job_id, uniqueness_mode, args].join(DATA_SEPARATOR)))
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
                next if (name =~ /^#{ActiveJob::Base.queue_name_prefix}/i).blank?
                output[name] = 0
                cursor = '0'

                loop do
                  cursor, fields = conn.hscan("uniqueness:#{name}", cursor, count: 100)

                  fields.each do |uniqueness_id, uniqueness|
                    should_clean_it = dirty_uniqueness?(uniqueness)
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

          def cleanup_uniqueness_all(limit = 10_000)
            queue_names = Sidekiq::Queue.all.map(&:name)
            output = {}

            Sidekiq.redis_pool.with do |conn|
              queue_names.each do |name|
                next if (name =~ /^#{ActiveJob::Base.queue_name_prefix}/i).blank?
                output[name] = 0
                cursor = '0'

                loop do
                  cursor, fields = conn.hscan("uniqueness:#{name}", cursor, count: 100)

                  fields.each do |uniqueness_id, _uniqueness|
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
                next if (name =~ /^#{ActiveJob::Base.queue_name_prefix}/i).blank?
                output[name] = 0

                (from..to).each do |day|
                  %i[enqueue perform].each do |stage|
                    klasses = conn.hkeys("jobstats:#{day}:#{stage}_attempted:#{name}")

                    klasses.each do |klass|
                      %i[attempted skipped processing failed processed].each do |progress|
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

        def enqueue_stage?(*args)
          self.class.enqueue_stage?(*args)
        end

        def perform_stage?(*args)
          self.class.perform_stage?(*args)
        end

        def enqueue_stage_job?(*args)
          self.class.enqueue_stage_job?(*args)
        end

        def perform_stage_job?(*args)
          self.class.perform_stage_job?(*args)
        end

        def dirty_uniqueness?(*args)
          self.class.dirty_uniqueness?(*args)
        end

        def read_uniqueness(*args)
          self.class.read_uniqueness(*args)
        end

        def read_uniqueness_dump(*args)
          self.class.read_uniqueness_dump(*args)
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
