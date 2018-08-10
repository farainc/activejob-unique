class ActiveJobs::WithTimeoutJob < ApplicationJob
  include ActiveJobs::JobStats

  queue_as :with_timeout
  unique_for 30.minutes, true

  def perform(args, *_args)
    run(args)

    true
  end
end
