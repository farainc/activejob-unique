class ActiveJobs::WithUniqueJob < ApplicationJob
  include ActiveJobs::JobStats

  queue_as :with_unique
  unique_for true, true

  def perform(args, *_args)
    run(args)

    true
  end
end
