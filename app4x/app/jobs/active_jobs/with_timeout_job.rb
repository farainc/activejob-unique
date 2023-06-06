class ActiveJobs::WithTimeoutJob < ApplicationJob
  include ActiveJobs::JobStats

  queue_as :with_timeout
  unique_for 30.minutes, debug: true

  def perform(job_args, *_args)
    run(job_args)

    true
  end
end
