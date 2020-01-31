class ActiveJobs::WithUniqueJob < ApplicationJob
  include ActiveJobs::JobStats

  queue_as :with_unique
  unique_for true, true

  def perform(job_args, *_args)
    run(job_args)

    true
  end
end
