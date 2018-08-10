class ActiveJobs::EnqueueJob < ApplicationJob
  queue_as :default

  def perform(klass_name, args_list)
    klass = klass_name.constantize

    args_list.each do |args|
      klass.perform_later(args)
    end

    true
  end

  def self.enqueue_multiple(total = 100, times = 10)
    [
      ActiveJobs::WithEnqueueOnlyJob,
      ActiveJobs::WithPerformOnlyJob,
      ActiveJobs::WithTimeoutJob,
      ActiveJobs::WithUniqueJob,
      ActiveJobs::WithoutUniqueJob
    ].each do |j|
      j.enqueue_multiple(total, times)
    end

    true
  end
end
