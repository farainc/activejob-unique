class ActiveJobs::EnqueueJob < ApplicationJob
  queue_as :default

  def perform(klass_name, args_list)
    klass = klass_name.constantize

    args_list.each do |job_args|
      klass.perform_later(job_args)
    end

    true
  end

  def self.enqueue_multiple(total = 10, times = 100)
    ActiveJobs::WithPerformOnlyJob.enqueue_multiple(10, 100)
    sleep(30)

    [
      ActiveJobs::WithEnqueueOnlyJob,
      ActiveJobs::WithTimeoutJob,
      ActiveJobs::WithUniqueJob,
      ActiveJobs::WithoutUniqueJob
    ].each do |j|
      j.enqueue_multiple(total, times)
    end

    true
  end
end
