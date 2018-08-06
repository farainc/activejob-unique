require_relative 'unique/compatible'
require_relative 'unique/api'
require_relative 'unique/core'
require_relative 'unique/queue_adapters'

require_relative 'unique/version'

module ActiveJob
  module Unique
    extend ActiveSupport::Autoload

    UNIQUENESS_MODE_WHILE_EXECUTING = :while_executing
    UNIQUENESS_MODE_UNTIL_TIMEOUT = :until_timeout
    UNIQUENESS_MODE_UNTIL_EXECUTING = :until_executing
    UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING = :until_and_while_executing

    PROGRESS_STAGE_ENQUEUE_ATTEMPTED = :enqueue_attempted
    PROGRESS_STAGE_ENQUEUE_PROCESSING = :enqueue_processing
    PROGRESS_STAGE_ENQUEUE_FAILED = :enqueue_failed
    PROGRESS_STAGE_ENQUEUE_PROCESSED = :enqueue_processed
    PROGRESS_STAGE_ENQUEUE_SKIPPED = :enqueue_skipped

    PROGRESS_STAGE_PERFORM_ATTEMPTED = :perform_attempted
    PROGRESS_STAGE_PERFORM_PROCESSING = :perform_processing
    PROGRESS_STAGE_PERFORM_FAILED = :perform_failed
    PROGRESS_STAGE_PERFORM_PROCESSED = :perform_processed
    PROGRESS_STAGE_PERFORM_SKIPPED = :perform_skipped
    PROGRESS_STAGE_PERFORM_EXPIRED = :perform_expired

    PROGRESS_STAGE_ENQUEUE = [
      PROGRESS_STAGE_ENQUEUE_ATTEMPTED,
      PROGRESS_STAGE_ENQUEUE_PROCESSING,
      PROGRESS_STAGE_ENQUEUE_PROCESSED,
      PROGRESS_STAGE_ENQUEUE_SKIPPED,
      PROGRESS_STAGE_ENQUEUE_FAILED
    ]

    PROGRESS_STAGE_PERFORM = [
      PROGRESS_STAGE_PERFORM_ATTEMPTED,
      PROGRESS_STAGE_PERFORM_PROCESSING,
      PROGRESS_STAGE_PERFORM_PROCESSED,
      PROGRESS_STAGE_PERFORM_SKIPPED,
      PROGRESS_STAGE_PERFORM_FAILED
    ]

    PROGRESS_STAGE = PROGRESS_STAGE_ENQUEUE + PROGRESS_STAGE_PERFORM

    PROGRESS_STATE_EXPIRATION = 30.seconds
    PROGRESS_STATS_SEPARATOR = 0x1E.chr
    PROGRESS_STATS_PREFIX = :job_progress_stats

    autoload :Compatible
    autoload :Api
    autoload :Core
    autoload :QueueAdapters
  end
end
