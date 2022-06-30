require_dependency 'db/active_job_unique'

module ActiveJobs
  module JobStats
    extend ActiveSupport::Concern

    included do
      around_enqueue do |job, block|
        db_job = Db::ActiveJobUnique.find_by(job_name: self.class.name, job_args: job.arguments[0])
        Db::ActiveJobUnique.update_counters(db_job.id, around_enqueue: 1) if db_job.present?

        block.call
      end

      around_perform do |job, block|
        db_job = Db::ActiveJobUnique.find_by(job_name: self.class.name, job_args: job.arguments[0])
        Db::ActiveJobUnique.update_counters(db_job.id, around_perform: 1) if db_job.present?

        block.call
      end
    end

    module ClassMethods
      def enqueue_multiple(total = 100, times = 10)
        prepare_multiple(total)

        (1..times).each do
          (1..total).to_a.shuffle.each do |job_args|
            ActiveJobs::EnqueueJob.perform_later(self.name, [job_args])
          end
        end

        true
      end

      def enqueue_one
        db_job = Db::ActiveJobUnique.find_one_or_create_by!(job_name: self.name, job_args: 1)
        db_job.update_columns(around_enqueue: 0, around_perform: 0, performed: 0)

        ActiveJobs::EnqueueJob.perform_later(self.name, [1])
      end

      def prepare_multiple(total)
        (1..total).each do |job_args|
          db_job = Db::ActiveJobUnique.find_one_or_create_by!(job_name: self.name, job_args: job_args)
          db_job.update_columns(around_enqueue: 0, around_perform: 0, performed: 0)
        end
      end
    end

    def run(job_args, sleep_time = rand(0...10))
      db_job = Db::ActiveJobUnique.find_by(job_name: self.class.name, job_args: job_args)
      return false if db_job.blank?

      sleep(sleep_time / 10.00)

      Db::ActiveJobUnique.update_counters(db_job.id, performed: 1)
    end
  end
end
