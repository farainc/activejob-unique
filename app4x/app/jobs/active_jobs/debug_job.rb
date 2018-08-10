class ActiveJobs::DebugJob < ApplicationJob
  queue_as :default

  def perform(*args)
    Sidekiq.redis_pool.with do |conn|


      byebug
    end

    byebug
  end

  before_enqueue do |job|
  end

  before_perform do |job|
  end

  PROGRESS_STATE_EXPIRATION = 30.seconds
  PROGRESS_STATS_SEPARATOR = 0x1E.chr
  PROGRESS_STATS_PREFIX = :job_progress_stats

  DAY_SCORE_BASE    = 100_000_000_000_000
  QUEUE_SCORE_BASE  = 10_000_000_000_000
  DAILY_SCORE_BASE  = 1_000_000_000
  UNIQUENESS_ID_SCORE_BASE = 10_000

  def valid_uniqueness_mode?(uniqueness_mode)
    [UNIQUENESS_MODE_WHILE_EXECUTING,
     UNIQUENESS_MODE_UNTIL_EXECUTING,
     UNIQUENESS_MODE_UNTIL_AND_WHILE_EXECUTING,
     UNIQUENESS_MODE_UNTIL_TIMEOUT].include?(uniqueness_mode.to_s.to_sym)
  end

  def enqueue_only_uniqueness_mode?(uniqueness_mode)
    UNIQUENESS_MODE_UNTIL_EXECUTING == uniqueness_mode.to_s.to_sym
  end

  def perform_only_uniqueness_mode?(uniqueness_mode)
    UNIQUENESS_MODE_WHILE_EXECUTING == uniqueness_mode.to_s.to_sym
  end

  def until_timeout_uniqueness_mode?(uniqueness_mode)
    UNIQUENESS_MODE_UNTIL_TIMEOUT == uniqueness_mode.to_s.to_sym
  end

  def sequence_day(now)
    now.to_date.strftime('%Y%m%d').to_i
  end

  def sequence_today
    sequence_day(Time.now.utc)
  end

  def job_progress_stats
    "#{PROGRESS_STATS_PREFIX}:stats"
  end

  def job_progress_stats_jobs
    "#{job_progress_stats}:jobs"
  end

  def job_progress_stats_job_key(job_name, queue_name, progress_stage)
    "#{job_name}#{PROGRESS_STATS_SEPARATOR}#{queue_name}#{PROGRESS_STATS_SEPARATOR}#{progress_stage}"
  end

  def job_progress_stage_state
    "#{PROGRESS_STATS_PREFIX}:state"
  end

  def job_progress_stage_state_key(job_name, queue_name, uniqueness_id, stage)
    [
      job_progress_stage_state,
      job_name,
      queue_name,
      uniqueness_id,
      stage
    ].join(PROGRESS_STATS_SEPARATOR)
  end

  def job_progress_stage_state_field(job_name, queue_name, uniqueness_id)
    [
      job_name,
      queue_name,
      uniqueness_id
    ].join(PROGRESS_STATS_SEPARATOR)
  end

  def job_progress_stage_logs
    "#{job_progress_stage_state}:logs"
  end

  def job_progress_stage_log_key(job_name)
    "#{job_progress_stage_logs}#{PROGRESS_STATS_SEPARATOR}#{job_name}"
  end

  def regroup_job_progress_stats(job_progress_stats_key, job_names, conn)
    stats_job_group = {}

    conn.hscan_each(job_progress_stats_key, count: 1000) do |name, value|
      job_name, queue_name, progress_stage = name.to_s.split(PROGRESS_STATS_SEPARATOR)
      next unless job_names.include?(job_name)

      stage, progress = progress_stage.split('_')
      next if queue_name.to_s.empty? || stage.to_s.empty? || progress.to_s.empty?

      stats_job_group[job_name] ||= {}
      stats_job_group[job_name][queue_name] ||= {}
      stats_job_group[job_name][queue_name]['enqueue'] ||= {}
      stats_job_group[job_name][queue_name]['perform'] ||= {}

      stats_job_group[job_name][queue_name][stage][progress] = value
    end

    stats_job_group
  end

  def cleanup_yesterday_progress_stats(conn, now)
    day = sequence_day(now - 3600 * 24)
    conn.del("#{job_progress_stats}:#{day}")
  end

  def group_job_progress_stage_uniqueness_flag_keys(conn, job_names)
    state_key = job_progress_stage_state

    uniqueness_keys = {}
    i = 0

    conn.hscan_each(state_key, count: 1000) do |name, value|
      i += 1

      job_name, queue_name, _ = name.to_s.split(PROGRESS_STATS_SEPARATOR)
      next unless job_names.include?(job_name)
      next if queue_name.to_s.empty?

      progress_stage, _, _ = value.to_s.split(PROGRESS_STATS_SEPARATOR)
      stage, _ = progress_stage.split('_')
      next if stage.to_s.empty?

      uniqueness_keys[job_name] ||= {}
      uniqueness_keys[job_name][queue_name] ||= {}
      uniqueness_keys[job_name][queue_name][stage] ||= 0

      uniqueness_keys[job_name][queue_name][stage] += 1

      # maxmium 10,000
      break if i >= 10_000
    end

    uniqueness_keys
  end

  def query_job_progress_stage_state_uniqueness(conn, job_name, queue_name, stage, count, begin_index)
    state_key = job_progress_stage_state
    match_filter = [job_name, queue_name, "*"].join(PROGRESS_STATS_SEPARATOR)

    i = 0
    begin_index += 1
    end_index = begin_index + count
    job_stats = []
    next_page_availabe = false

    conn.hscan_each(state_key, match: match_filter, count: 100) do |name, value|
      next if stage != '*' && (value =~ /^#{stage}/i).nil?

      i += 1
      next if i < begin_index

      if i < end_index
        n_job_name, n_queue_name, uniqueness_id = name.to_s.split(PROGRESS_STATS_SEPARATOR)
        progress_stage, timestamp, job_id = value.to_s.split(PROGRESS_STATS_SEPARATOR)

        stats = {
          job_name: n_job_name,
          queue: n_queue_name,
          progress_stage: progress_stage,
          uniqueness_id: uniqueness_id,
          timestamp: Time.at(timestamp.to_f).utc,
          job_id: job_id
        }

        job_stats << stats
      else
        next_page_availabe = true

        break
      end
    end

    [next_page_availabe, job_stats]
  end

  def cleanup_job_progress_state_uniqueness(conn, job_name, queue_name, stage, uniqueness_id)
    cursor = 0
    state_key = job_progress_stage_state
    match_filter = [job_name, queue_name, uniqueness_id, "*"].join(PROGRESS_STATS_SEPARATOR)

    if stage != '*'
      conn.hscan_each(state_key, match: filter, count: 100) do |name, value|
        next if (value =~ /^#{stage}/i).nil?
        conn.hdel(name)
      end
    else
      loop do
        cursor, key_values = conn.hscan(state_key, cursor, match: filter, count: 100)
        conn.hdel(*key_values.map{|kv| kv[0]})

        break if cursor == '0'
      end
    end

    true
  end

  def query_job_progress_stage_state_processing(conn, job_name, queue_name, count, begin_index)
    match_filter = [job_progress_stage_state, job_name, queue_name, "*"].join(PROGRESS_STATS_SEPARATOR)

    i = 0
    begin_index += 1
    end_index = begin_index + count
    job_stats = []
    next_page_availabe = false

    conn.scan_each(match: match_filter, count: 100) do |name, value|
      i += 1
      next if i < begin_index

      if i < end_index
        _, n_job_name, n_queue_name, uniqueness_id, progress_stage = name.to_s.split(PROGRESS_STATS_SEPARATOR)

        stats = {
          job_name: n_job_name,
          queue: n_queue_name,
          progress_stage: progress_stage,
          uniqueness_id: uniqueness_id,
          expires: Time.at(value.to_f).utc
        }

        job_stats << stats
      else
        next_page_availabe = true

        break
      end
    end

    [next_page_availabe, job_stats]
  end

  def cleanup_job_progress_state_processing(conn, job_name, queue_name, uniqueness_id)
    cursor = 0
    match_filter = [job_progress_stage_state, job_name, queue_name, uniqueness_id, "*"].join(PROGRESS_STATS_SEPARATOR)

    loop do
      cursor, keys = conn.scan(cursor, match: match_filter, count: 100)

      conn.del(*keys)

      break if cursor == '0'
    end

    true
  end

  def group_job_progress_stage_log_keys(conn, job_stats_all_time)
    job_log_keys = {}

    job_stats_all_time.each do |job_name, queues|
      job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
      next unless conn.exists(job_score_key)

      job_log_keys[job_name] = {}

      queues.keys.each do |queue_name|
        min = conn.zscore(job_score_key, "queue:#{queue_name}")
        next if min.nil?

        job_log_keys[job_name][queue_name] = true
      end
    end

    job_log_keys
  end

  def query_job_progress_stage_log_jobs(conn, day, job_name, queue_name, uniqueness_id, count, begin_index)
    next_page_availabe = false

    job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
    return [false, []] unless conn.exists(job_score_key)

    day_score = ((day % 8) + 1) * DAY_SCORE_BASE

    queue_id_score = conn.zscore(job_score_key, "queue:#{queue_name}").to_i
    queue_id_score = (queue_id_score % 9) * QUEUE_SCORE_BASE

    uniqueness_id_score = conn.zscore(job_score_key, "uniqueness_id:#{uniqueness_id}").to_i
    uniqueness_id_score *= UNIQUENESS_ID_SCORE_BASE

    min_score = day_score + queue_id_score + uniqueness_id_score

    max_score = if uniqueness_id_score > 0
                  min_score + UNIQUENESS_ID_SCORE_BASE
                elsif queue_id_score > 0
                  min_score + QUEUE_SCORE_BASE
                else
                  min_score + DAY_SCORE_BASE
                end

    job_logs = conn.zrevrangebyscore(
      job_score_key,
      "(#{max_score}",
      min_score,
      limit: [begin_index, begin_index + count + 1]
    )

    [job_logs.size > count, job_logs[0..count - 1]]
  end

  def query_job_progress_stage_log_job_one(conn, day, job_name, queue_name, uniqueness_id, job_id)
    job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
    return { logs: [], args: {} } unless conn.exists(job_score_key)

    job_log_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"
    return { logs: [], args: {} } unless conn.exists(job_log_key)

    log_data_key = job_progress_stage_log_key(job_name)
    log_data_field = "#{day % 9}#{PROGRESS_STATS_SEPARATOR}#{uniqueness_id}"

    job_id_score = conn.zscore(job_score_key, "#{queue_name}:#{uniqueness_id}:#{job_id}").to_i
    begin_index = 0
    completed = false

    job_logs = []

    loop do
      temp_logs = conn.zrangebyscore(
        job_log_key,
        job_id_score,
        "(#{job_id_score + 1}",
        limit: [begin_index, 101]
      )

      temp_logs.each do |log|
        next unless (log =~ /^#{job_id}#{PROGRESS_STATS_SEPARATOR}/i) == 0

        id, progress_stage, timestamp, reason, mode, expiration, expires, debug = log.split(PROGRESS_STATS_SEPARATOR)

        job_logs << {
          progress_stage: progress_stage,
          timestamp: Time.at(timestamp.to_f).utc,
          reason: reason,
          mode: mode,
          expiration: expiration,
          expires: Time.at(expires.to_f).utc,
          debug: debug
        }
        completed = %w[enqueue_skipped
                       enqueue_failed
                       perform_skipped
                       perform_failed
                       perform_processed].include?(progress_stage)

        break if completed
      end

      # update index offset
      begin_index += temp_logs.size

      # break if completed || search to the end.
      break if completed || temp_logs.size <= 100
    end

    args = JSON.parse(conn.hget(log_data_key, log_data_field)) rescue {}

    { logs: job_logs, args: args }
  end

  def cleanup_job_progress_stage_logs(conn, day, job_name, queue_name, uniqueness_id)
    job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
    return unless conn.exists(job_score_key)

    job_log_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"
    return unless conn.exists(job_log_key)

    day_score = ((day % 8) + 1) * DAY_SCORE_BASE

    queue_id_score = conn.zscore(job_score_key, "queue:#{queue_name}").to_i
    queue_id_score = (queue_id_score % 9) * QUEUE_SCORE_BASE

    uniqueness_id_score = conn.zscore(job_score_key, "uniqueness_id:#{uniqueness_id}").to_i
    uniqueness_id_score *= UNIQUENESS_ID_SCORE_BASE

    min_score = day_score + queue_id_score + uniqueness_id_score

    max_score = if uniqueness_id_score > 0
                  min_score + UNIQUENESS_ID_SCORE_BASE
                elsif queue_id_score > 0
                  min_score + QUEUE_SCORE_BASE
                else
                  min_score + DAY_SCORE_BASE
                end

    conn.zremrangebyscore(
      job_score_key,
      min_score,
      "(#{max_score}"
    )

    conn.zremrangebyscore(
      job_log_key,
      min_score,
      "(#{max_score}"
    )

    true
  end

  def cleanup_job_progress_stage_log_one(conn, _day, job_name, queue_name, uniqueness_id, job_id)
    job_score_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_score"
    return unless conn.exists(job_score_key)

    job_log_key = "#{job_progress_stage_log_key(job_name)}#{PROGRESS_STATS_SEPARATOR}job_logs"
    return unless conn.exists(job_log_key)

    job_id_score = conn.zscore(job_score_key, "#{queue_name}:#{uniqueness_id}:#{job_id}").to_i

    begin_index = 0
    completed = false

    loop do
      temp_logs = conn.zrangebyscore(
        job_log_key,
        job_id_score,
        "(#{job_id_score + 1}",
        limit: [begin_index, 101]
      )

      temp_logs.each do |log|
        next unless (log =~ /^#{job_id}#{PROGRESS_STATS_SEPARATOR}/i) == 0
        id, progress_stage, timestamp = log.split(PROGRESS_STATS_SEPARATOR)

        completed = %w[enqueue_skipped
                       enqueue_failed
                       perform_skipped
                       perform_failed
                       perform_processed].include?(progress_stage)

        conn.zrem(job_log_key, log)

        break if completed
      end

      # update index offset
      begin_index += temp_logs.size

      # break if completed || search to the end.
      break if completed || temp_logs.size <= 100
    end

    conn.zrem(job_score_key, "#{queue_name}:#{uniqueness_id}:#{job_id}")

    true
  end
end
