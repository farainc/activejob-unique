module ActiveJob
  module Unique
    module Web
      module SidekiqWeb
        module Server
          def self.registered(app)
            # index page of stats
            app.get '/job_stats' do
              view_path = File.join(File.expand_path('..', __FILE__), 'views')

              today = SidekiqWeb.sequence_today

              @job_stats = {}
              @job_stats_today = {}
              @job_stats_all_time = {}
              @state_keys = {}
              @count = (params[:count] || 10).to_i
              @current_page = params[:page].to_i
              @current_page = 1 if @current_page < 1

              Sidekiq.redis_pool.with do |conn|
                @total_size = conn.zcount(SidekiqWeb.job_progress_stats_jobs, '-inf', '+inf')
              end

              begin_index = (@current_page - 1) * @count
              if begin_index > @total_size
                begin_index = 0
                @current_page = 1
              end
              end_index = begin_index + @count - 1


              Sidekiq.redis_pool.with do |conn|
                job_names = conn.zrevrange(SidekiqWeb.job_progress_stats_jobs, begin_index, end_index)
                @state_keys = SidekiqWeb.job_progress_state_all_keys(conn, job_names)

                @job_stats_all_time = SidekiqWeb.regroup_job_progress_stats(SidekiqWeb.job_progress_stats, job_names, conn)
                @job_stats_today = SidekiqWeb.regroup_job_progress_stats("#{SidekiqWeb.job_progress_stats}:#{today}", job_names, conn)

                @job_stats = job_names
              end

              render(:erb, File.read(File.join(view_path, 'index.erb')))
            end

            app.get '/job_stats/uniqueness/:job_name' do
              view_path = File.join(File.expand_path('..', __FILE__), 'views')

              @job_name = route_params[:job_name]
              @job_stats = []

              @count = (params[:count] || 100).to_i
              @current_page = params[:page].to_i
              @current_page = 1 if @current_page < 1

              begin_index = (@current_page - 1) * @count
              next_page_availabe = false

              Sidekiq.redis_pool.with do |conn|
                next_page_availabe, @job_stats = SidekiqWeb.job_progress_state_group_stats(conn, @job_name, @count, begin_index)
              end

              @total_size = @count * (@current_page - 1) + @job_stats.size
              @total_size += 1 if next_page_availabe

              render(:erb, File.read(File.join(view_path, 'uniqueness.erb')))
            end

            # delete uniqueness job
            app.post '/job_stats/uniqueness/:job_name/:queue_name/:stage_key/:uniqueness_id/delete' do
              job_name = route_params[:job_name]
              queue_name = route_params[:queue_name]
              stage_key = route_params[:stage_key]
              uniqueness_id = route_params[:uniqueness_id]

              Sidekiq.redis_pool.with do |conn|
                SidekiqWeb.cleanup_job_progress_state_one(
                  conn,
                  job_name,
                  uniqueness_id,
                  queue_name,
                  stage_key
                )
              end

              redirect URI(request.referer).path
            end

            # delete job_stats uniquess for a job
            app.post '/job_stats/uniqueness/:job_name/delete' do
              job_name = route_params[:job_name]

              Sidekiq.redis_pool.with do |conn|
                SidekiqWeb.cleanup_job_progress_state_group(conn, job_name)
              end

              redirect URI(request.referer).path
            end

            #
            # app.get '/job_stats/:stage/:queue_name' do
            #   stage = route_params[:stage]
            #   queue_name = route_params[:queue_name]
            #
            #   view_path = File.join(File.expand_path('..', __FILE__), 'views')
            #
            #   today = ActiveJob::JobStats::SidekiqExtension.sequence_today
            #   @queue_name = queue_name
            #   @stage = stage
            #   @job_stats = []
            #   @switch_stage = case @stage
            #                   when 'enqueue'
            #                     'perform'
            #                   when 'perform'
            #                     'enqueue'
            #                   end
            #
            #   @count = (params[:count] || 10).to_i
            #   @current_page = params[:page].to_i
            #   @current_page = 1 if @current_page < 1
            #
            #   Sidekiq.redis_pool.with do |conn|
            #     @total_size = conn.hlen("jobstats:#{stage}_attempted:#{queue_name}")
            #   end
            #
            #   begin_index = (@current_page - 1) * @count
            #   if begin_index > @total_size
            #     begin_index = 0
            #     @current_page = 1
            #   end
            #   end_index = begin_index + @count - 1
            #
            #   job_stats_hash = {}
            #   job_klasses = []
            #   stats_hash = { skipped: [], processing: [], processed: [], failed: [] }
            #   today_stats_hash = { attempted: [], skipped: [], processing: [], processed: [], failed: [] }
            #
            #   Sidekiq.redis_pool.with do |conn|
            #     cursor, attempted_data = conn.hscan("jobstats:#{stage}_attempted:#{queue_name}", begin_index.to_s, count: @count)
            #     attempted_data = attempted_data[begin_index..end_index] if cursor == '0'
            #     attempted_data.map do |k, v|
            #       job_stats_hash[k] = {
            #         klass: k,
            #         attempted: { all: v.to_i },
            #         skipped: {},
            #         processing: {},
            #         processed: {},
            #         failed: {}
            #       }
            #     end
            #
            #     job_klasses = job_stats_hash.keys
            #
            #     unless job_klasses.empty?
            #       # today attempted
            #       today_stats_hash[:attempted] = conn.hmget("jobstats:#{today}:#{stage}_attempted:#{queue_name}", job_klasses)
            #
            #       # all left status
            #       stats_hash.keys.each do |status|
            #         stats_hash[status] = conn.hmget("jobstats:#{stage}_#{status}:#{queue_name}", job_klasses)
            #         today_stats_hash[status] = conn.hmget("jobstats:#{today}:#{stage}_#{status}:#{queue_name}", job_klasses)
            #       end
            #     end
            #   end
            #
            #   statuses = stats_hash.keys
            #   today_statuses = today_stats_hash.keys
            #
            #   job_klasses.each_with_index do |klass, i|
            #     statuses.map { |s| job_stats_hash[klass][s][:all] = stats_hash[s][i].to_i }
            #     today_statuses.map { |s| job_stats_hash[klass][s][:today] = today_stats_hash[s][i].to_i }
            #   end
            #
            #   @job_stats = job_stats_hash.values
            #
            #   render(:erb, File.read(File.join(view_path, 'stage.erb')))
            # end
            #

            #
            # # delete uniqueness jobs per queue
            # app.post '/uniqueness/:queue_name/delete' do
            #   queue_name = route_params[:queue_name]
            #
            #   ActiveJob::JobStats::SidekiqExtension.cleanup_hash_set("uniqueness:#{queue_name}")
            #   ActiveJob::JobStats::SidekiqExtension.cleanup_hash_set("uniqueness:dump:#{queue_name}")
            #
            #   redirect URI(request.referer).path
            # end
            #
            # # delete uniqueness job
            # app.post '/uniqueness/:queue_name/:uniqueness_id/delete' do
            #   queue_name = route_params[:queue_name]
            #   uniqueness_id = route_params[:uniqueness_id]
            #
            #   Sidekiq.redis_pool.with do |conn|
            #     conn.hdel("uniqueness:#{queue_name}", uniqueness_id)
            #     conn.hdel("uniqueness:dump:#{queue_name}", uniqueness_id)
            #   end
            #
            #   redirect URI(request.referer).path
            # end

          end
        end
      end
    end
  end
end

if defined?(Sidekiq::Web)
  Sidekiq::Web.register ActiveJob::Unique::Web::SidekiqWeb::Server

  if Sidekiq::Web.tabs.is_a?(Array)
    # For sidekiq < 2.5
    Sidekiq::Web.tabs << 'Job Stats'
  else
    Sidekiq::Web.tabs['Job Stats'] = 'job_stats'
  end
end
