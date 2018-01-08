module ActiveJob
  module JobStats
    module SidekiqExtension
      module Web
        DATA_SEPARATOR = 0x1E.chr
        def self.registered(app)
          # index page of stats
          app.get '/job_stats' do
            view_path = File.join(File.expand_path('..', __FILE__), 'views')

            @job_stats = []
            today = ActiveJob::JobStats::SidekiqExtension.sequence_today

            Sidekiq.redis_pool.with do |conn|
              Sidekiq::Queue.all.each do |queue|
                @job_stats << {
                  queue_name: queue.name,
                  uniqueness: conn.hlen("uniqueness:#{queue.name}"),

                  today_enqueue_attempted: conn.hlen("jobstats:#{today}:enqueue_attempted:#{queue.name}"),
                  today_enqueue_skiped: conn.hlen("jobstats:#{today}:enqueue_skiped:#{queue.name}"),
                  today_enqueue_processing: conn.hlen("jobstats:#{today}:enqueue_processing:#{queue.name}"),
                  today_enqueue_processed: conn.hlen("jobstats:#{today}:enqueue_processed:#{queue.name}"),
                  today_enqueue_failed: conn.hlen("jobstats:#{today}:enqueue_failed:#{queue.name}"),

                  today_perform_attempted: conn.hlen("jobstats:#{today}:perform_attempted:#{queue.name}"),
                  today_perform_skiped: conn.hlen("jobstats:#{today}:perform_skiped:#{queue.name}"),
                  today_perform_processing: conn.hlen("jobstats:#{today}:perform_processing:#{queue.name}"),
                  today_perform_processed: conn.hlen("jobstats:#{today}:perform_processed:#{queue.name}"),
                  today_perform_failed: conn.hlen("jobstats:#{today}:perform_failed:#{queue.name}")
                }
              end
            end

            render(:erb, File.read(File.join(view_path, 'index.erb')))
          end

          app.get '/job_stats/uniqueness/:queue_name' do
            queue_name = route_params[:queue_name]

            view_path = File.join(File.expand_path('..', __FILE__), 'views')

            @queue_name = queue_name
            @job_stats = []

            @count = (params[:count] || 10).to_i
            @current_page = params[:page].to_i
            @current_page = 1 if @current_page < 1

            Sidekiq.redis_pool.with do |conn|
              @total_size = conn.hlen("uniqueness:#{queue_name}")
            end

            begin_index = (@current_page - 1) * @count
            if begin_index > @total_size
              begin_index = 0
              @current_page = 1
            end
            end_index = begin_index + @count - 1

            uniqueness_hash = {}
            uniqueness_ids = []
            uniqueness_dumps = []

            Sidekiq.redis_pool.with do |conn|
              cursor, uniqueness_data = conn.hscan("uniqueness:#{queue_name}", begin_index.to_s, count: @count)
              uniqueness_data = uniqueness_data[begin_index..end_index] if cursor == '0'
              uniqueness_data.map { |k, v| uniqueness_hash[k] = { progress_raw: v } }

              uniqueness_ids = uniqueness_hash.keys

              unless uniqueness_ids.empty?
                uniqueness_dumps = conn.hmget("uniqueness:dump:#{queue_name}", uniqueness_ids)
              end
            end

            uniqueness_ids.each_with_index do |uniqueness_id, i|
              progress_array = uniqueness_hash[uniqueness_id][:progress_raw].to_s.encode('utf-8', invalid: :replace, undef: :replace, replace: '').split(DATA_SEPARATOR)
              dump_array = uniqueness_dumps[i].to_s.encode('utf-8', invalid: :replace, undef: :replace, replace: '').split(DATA_SEPARATOR)

              progress, expires, defaults = progress_array
              klass, job_id, uniqueness_mode, *args = dump_array

              expires = expires.to_i
              expires = Time.at(expires).utc if expires.positive?

              defaults = defaults.to_i
              defaults = Time.at(defaults).utc if defaults.positive?

              @job_stats << {
                uniqueness_id: uniqueness_id,
                uniqueness_mode: uniqueness_mode,
                progress: progress,
                klass: klass,
                args: args,
                job_id: job_id,
                expires: expires,
                defaults: defaults
              }
            end

            render(:erb, File.read(File.join(view_path, 'uniqueness.erb')))
          end

          app.get '/job_stats/:stage/:queue_name' do
            stage = route_params[:stage]
            queue_name = route_params[:queue_name]

            view_path = File.join(File.expand_path('..', __FILE__), 'views')

            today = ActiveJob::JobStats::SidekiqExtension.sequence_today
            @queue_name = queue_name
            @stage = stage
            @job_stats = []
            @switch_stage = case @stage
                            when 'enqueue'
                              'perform'
                            when 'perform'
                              'enqueue'
                            end

            @count = (params[:count] || 10).to_i
            @current_page = params[:page].to_i
            @current_page = 1 if @current_page < 1

            Sidekiq.redis_pool.with do |conn|
              @total_size = conn.hlen("jobstats:#{stage}_attempted:#{queue_name}")
            end

            begin_index = (@current_page - 1) * @count
            if begin_index > @total_size
              begin_index = 0
              @current_page = 1
            end
            end_index = begin_index + @count - 1

            job_stats_hash = {}
            job_klasses = []
            stats_hash = { skiped: [], processing: [], processed: [], failed: [] }
            today_stats_hash = { attempted: [], skiped: [], processing: [], processed: [], failed: [] }

            Sidekiq.redis_pool.with do |conn|
              cursor, attempted_data = conn.hscan("jobstats:#{stage}_attempted:#{queue_name}", begin_index.to_s, count: @count)
              attempted_data = attempted_data[begin_index..end_index] if cursor == '0'
              attempted_data.map do |k, v|
                job_stats_hash[k] = {
                  klass: k,
                  attempted: { all: v.to_i },
                  skiped: {},
                  processing: {},
                  processed: {},
                  failed: {}
                }
              end

              job_klasses = job_stats_hash.keys

              unless job_klasses.empty?
                # today attempted
                today_stats_hash[:attempted] = conn.hmget("jobstats:#{today}:#{stage}_attempted:#{queue_name}", job_klasses)

                # all left status
                stats_hash.keys.each do |status|
                  stats_hash[status] = conn.hmget("jobstats:#{stage}_#{status}:#{queue_name}", job_klasses)
                  today_stats_hash[status] = conn.hmget("jobstats:#{today}:#{stage}_#{status}:#{queue_name}", job_klasses)
                end
              end
            end

            statuses = stats_hash.keys
            today_statuses = today_stats_hash.keys

            job_klasses.each_with_index do |klass, i|
              statuses.map { |s| job_stats_hash[klass][s][:all] = stats_hash[s][i].to_i }
              today_statuses.map { |s| job_stats_hash[klass][s][:today] = today_stats_hash[s][i].to_i }
            end

            @job_stats = job_stats_hash.values

            render(:erb, File.read(File.join(view_path, 'stage.erb')))
          end

          # delete job_stats per stage per queue
          app.post '/jobs_stats/:stage/:queue_name/delete' do
            stage = route_params[:stage]
            queue_name = route_params[:queue_name]

            [:skiped, :processing, :processed, :failed, :attempted].each do |status|
              ActiveJob::JobStats::SidekiqExtension.cleanup_hash_set("jobstats:#{stage}_#{status}:#{queue_name}")
            end

            redirect request.referer
          end

          # delete uniqueness jobs per queue
          app.post '/uniqueness/:queue_name/delete' do
            queue_name = route_params[:queue_name]

            ActiveJob::JobStats::SidekiqExtension.cleanup_hash_set("uniqueness:#{queue_name}")
            ActiveJob::JobStats::SidekiqExtension.cleanup_hash_set("uniqueness:dump:#{queue_name}")

            redirect request.referer
          end

          # delete uniqueness job
          app.post '/uniqueness/:queue_name/:uniqueness_id/delete' do
            queue_name = route_params[:queue_name]
            uniqueness_id = route_params[:uniqueness_id]

            Sidekiq.redis_pool.with do |conn|
              conn.hdel("uniqueness:#{queue_name}", uniqueness_id)
              conn.hdel("uniqueness:dump:#{queue_name}", uniqueness_id)
            end

            redirect request.referer
          end
        end
      end
    end
  end
end

if defined?(Sidekiq::Web)
  Sidekiq::Web.register ActiveJob::JobStats::SidekiqExtension::Web

  if Sidekiq::Web.tabs.is_a?(Array)
    # For sidekiq < 2.5
    Sidekiq::Web.tabs << 'Job Stats'
  else
    Sidekiq::Web.tabs['Job Stats'] = 'job_stats'
  end
end
