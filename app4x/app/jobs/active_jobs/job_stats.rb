require_dependency 'db/active_job_unique'

module ActiveJobs
  module JobStats
    extend ActiveSupport::Concern

    included do
      around_enqueue do |job, block|
        db_job = Db::ActiveJobUnique.find_by(job_name: self.class.name, args: job.arguments[0])
        Db::ActiveJobUnique.update_counters(db_job.id, around_enqueue: 1) if db_job.present?

        block.call
      end

      around_perform do |job, block|
        db_job = Db::ActiveJobUnique.find_by(job_name: self.class.name, args: job.arguments[0])
        Db::ActiveJobUnique.update_counters(db_job.id, around_perform: 1) if db_job.present?

        block.call
      end
    end

    module ClassMethods
      def enqueue_multiple(total = 100, times = 10)
        prepare_multiple(total)

        (1..times).each do
          (1..total).to_a.shuffle.each do |args|
            ActiveJobs::EnqueueJob.perform_later(self.name, [args])
          end
        end

        true
      end

      def enqueue_one
        db_job = Db::ActiveJobUnique.find_one_or_create_by!(job_name: self.name, args: 1)
        db_job.update_columns(around_enqueue: 0, around_perform: 0, performed: 0)

        ActiveJobs::EnqueueJob.perform_later(self.name, [1])
      end

      def prepare_multiple(total)
        (1..total).each do |args|
          db_job = Db::ActiveJobUnique.find_one_or_create_by!(job_name: self.name, args: args)
          db_job.update_columns(around_enqueue: 0, around_perform: 0, performed: 0)
        end
      end
    end

    def run(args)
      db_job = Db::ActiveJobUnique.find_by(job_name: self.class.name, args: args)
      return false if db_job.blank?

      sleep(rand(0...10) / 10.00)

      Db::ActiveJobUnique.update_counters(db_job.id, performed: 1)
    end
  end
end
