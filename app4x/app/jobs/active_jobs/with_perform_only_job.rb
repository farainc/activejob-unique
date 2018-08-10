class ActiveJobs::WithPerformOnlyJob < ApplicationJob
  include ActiveJobs::JobStats

  queue_as :with_perform_only
  unique_for :while_executing, true

  def perform(args, *_args)
    run(args)

    true
  end
end
