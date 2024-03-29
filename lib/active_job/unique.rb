require 'sidekiq'

require_relative 'unique/compatible'
require_relative 'unique/api'
require_relative 'unique/core'
require_relative 'unique/adapters'
require_relative 'unique/stats'

require_relative 'unique/version'

module ActiveJob
  module Unique
    UNIQUENESS_MODE_WHILE_EXECUTING = :while_executing
    UNIQUENESS_MODE_UNTIL_TIMEOUT = :until_timeout
    UNIQUENESS_MODE_UNTIL_EXECUTING = :until_executing
    UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING = :until_and_while_executing

    PROGRESS_STAGE_ENQUEUE_GROUP = :enqueue
    PROGRESS_STAGE_PERFORM_GROUP = :perform

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
    PROGRESS_STAGE_PERFORM_TIMEOUT = :perform_timeout

    PROGRESS_STAGE_ENQUEUE = [
      PROGRESS_STAGE_ENQUEUE_ATTEMPTED,
      PROGRESS_STAGE_ENQUEUE_PROCESSING,
      PROGRESS_STAGE_ENQUEUE_PROCESSED,
      PROGRESS_STAGE_ENQUEUE_SKIPPED,
      PROGRESS_STAGE_ENQUEUE_FAILED
    ].freeze

    PROGRESS_STAGE_PERFORM = [
      PROGRESS_STAGE_PERFORM_ATTEMPTED,
      PROGRESS_STAGE_PERFORM_PROCESSING,
      PROGRESS_STAGE_PERFORM_PROCESSED,
      PROGRESS_STAGE_PERFORM_SKIPPED,
      PROGRESS_STAGE_PERFORM_FAILED
    ].freeze

    PROGRESS_STAGE = PROGRESS_STAGE_ENQUEUE + PROGRESS_STAGE_PERFORM

    PROGRESS_STATE_EXPIRATION = 30
    PROGRESS_STATS_SEPARATOR = 0x1E.chr
    PROGRESS_STATS_PREFIX = :job_progress_stats

    DAY_SCORE_BASE    = 100_000_000_000_000
    QUEUE_SCORE_BASE  = 1_000_000_000_000
    DAILY_SCORE_BASE  = 10_000_000
    UNIQUENESS_ID_SCORE_BASE = 100_000

    ONE_DAY_SECONDS   = 86_400
  end
end
