class ActiveJobs::WithEnqueueOnlyJob < ApplicationJob
  include ActiveJobs::JobStats

  queue_as :with_enqueue_only
  unique_for :until_executing

  def perform(args, *_args)
    run(args)

    true
  end

  def self.enqueue_multiple
    total = 1000

    self.prepare_multiple(total)

    (1..total).to_a.shuffle.each do |args|
      ActiveJobs::EnqueueJob.perform_later(self.name, [args])
    end

    (1..total).to_a.shuffle.each do |args|
      ActiveJobs::EnqueueJob.perform_later(self.name, [args])
    end

    true
  end
end
