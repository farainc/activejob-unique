module ActiveJob
  module Unique
    module Web
      module SidekiqWeb
        module Server
          def self.registered(app)
            # index page of stats
            app.get '/job_stats' do
              view_path = File.join(File.expand_path('..', __FILE__), 'views')

              @today = SidekiqWeb.sequence_today

              @job_stats = {}
              @job_stats_today = {}
              @job_stats_all_time = {}
              @state_keys = {}
              @count = (params[:count] || 20).to_i
              @current_page = params[:page].to_i
              @current_page = 1 if @current_page < 1

              Sidekiq.redis_pool.with do |conn|
                @total_size = conn.zcount(SidekiqWeb.job_progress_stats_jobs, '-inf', '+inf')

                SidekiqWeb.uniqueness_cleanup_progress_stats(conn, Time.now.utc)                
              end

              begin_index = (@current_page - 1) * @count
              if begin_index > @total_size
                begin_index = 0
                @current_page = 1
              end
              end_index = begin_index + @count - 1

              Sidekiq.redis_pool.with do |conn|
                job_names = conn.zrevrange(SidekiqWeb.job_progress_stats_jobs, begin_index, end_index)
                @state_keys = SidekiqWeb.job_progress_state_uniqueness_keys(conn, job_names)

                @job_stats_all_time = SidekiqWeb.regroup_job_progress_stats(SidekiqWeb.job_progress_stats, job_names, conn)
                @job_stats_today = SidekiqWeb.regroup_job_progress_stats("#{SidekiqWeb.job_progress_stats}:#{@today}", job_names, conn)

                @job_log_keys = SidekiqWeb.job_progress_state_log_keys(conn, @job_stats_all_time)

                @job_stats = job_names
              end

              render(:erb, File.read(File.join(view_path, 'index.erb')))
            end

            app.get '/job_stats/uniqueness/:job_name' do
              view_path = File.join(File.expand_path('..', __FILE__), 'views')

              @job_name = route_params[:job_name]
              @job_stats = []

              @count = (params[:count] || 20).to_i
              @current_page = params[:page].to_i
              @current_page = 1 if @current_page < 1

              begin_index = (@current_page - 1) * @count
              next_page_availabe = false

              Sidekiq.redis_pool.with do |conn|
                next_page_availabe, @job_stats = SidekiqWeb.job_progress_state_group_stats(
                  conn,
                  @job_name,
                  @count,
                  begin_index)
              end

              @total_size = @count * (@current_page - 1) + @job_stats.size
              @total_size += 1 if next_page_availabe

              render(:erb, File.read(File.join(view_path, 'uniqueness.erb')))
            end

            # delete multiple uniqueness flag for a job
            app.post '/job_stats/uniqueness/:job_name/delete' do
              job_name = route_params[:job_name]

              Sidekiq.redis_pool.with do |conn|
                SidekiqWeb.cleanup_job_progress_state_group(conn, job_name)
              end

              redirect URI(request.referer).path
            end

            # delete single uniqueness flag
            app.post '/job_stats/uniqueness/:job_name/:queue_name/:stage_key/:uniqueness_id/delete' do
              job_name = route_params[:job_name]
              queue_name = route_params[:queue_name]
              stage_key = route_params[:stage_key]
              uniqueness_id = route_params[:uniqueness_id].to_s

              Sidekiq.redis_pool.with do |conn|
                SidekiqWeb.cleanup_job_progress_state_one(
                  conn,
                  job_name,
                  queue_name,
                  uniqueness_id,
                  stage_key
                )
              end

              redirect URI(request.referer).path
            end

            app.get '/job_stats/logs/:day/:job_name/:queue_name/:uniqueness_id' do
              view_path = File.join(File.expand_path('..', __FILE__), 'views')

              @job_logs = []
              @job_name = route_params[:job_name]
              @queue_name = route_params[:queue_name].to_s
              @day = route_params[:day].to_i
              @day = SidekiqWeb.sequence_today if @day < SidekiqWeb.sequence_day(Time.now.utc-3600*24*9)

              @uniqueness_id = route_params[:uniqueness_id].to_s
              @uniqueness_id = "*" unless @uniqueness_id.size == 32

              @count = (params[:count] || 20).to_i
              @current_page = params[:page].to_i
              @current_page = 1 if @current_page < 1

              begin_index = (@current_page - 1) * @count
              next_page_availabe = false

              Sidekiq.redis_pool.with do |conn|
                next_page_availabe, @job_logs = SidekiqWeb.job_progress_state_log_jobs(
                  conn,
                  @day,
                  @job_name,
                  @queue_name,
                  @uniqueness_id,
                  @count,
                  begin_index)

                  # when first page display, always clean logs data 8 days ago
                  if @current_page == 1 && @queue_name == "*" && @uniqueness_id == "*"
                    SidekiqWeb.cleanup_job_progress_state_logs(conn, SidekiqWeb.sequence_day(Time.now.utc-3600*24*8), @job_name, @queue_name, @uniqueness_id)
                  end
              end

              @total_size = @count * (@current_page - 1) + @job_logs.size
              @total_size += 1 if next_page_availabe

              render(:erb, File.read(File.join(view_path, 'logs.erb')))
            end

            app.get '/job_stats/logs/:day/:job_name/:queue_name/:uniqueness_id/:job_id' do
              view_path = File.join(File.expand_path('..', __FILE__), 'views')

              @job_log = {}
              @job_name = route_params[:job_name]
              @queue_name = route_params[:queue_name]
              @day = route_params[:day].to_i
              @uniqueness_id = route_params[:uniqueness_id].to_s
              @job_id = route_params[:job_id]

              Sidekiq.redis_pool.with do |conn|
                @job_log = SidekiqWeb.job_progress_state_log_job_one(
                  conn,
                  @day,
                  @job_name,
                  @queue_name,
                  @uniqueness_id,
                  @job_id
                )
              end

              render(:erb, File.read(File.join(view_path, 'log.erb')))
            end

            # delete all uniqueness logs
            app.post '/job_stats/logs/:day/:job_name/:queue_name/:uniqueness_id/delete' do
              job_name = route_params[:job_name]
              queue_name = route_params[:queue_name]
              day = route_params[:day].to_i
              uniqueness_id = route_params[:uniqueness_id].to_s

              Sidekiq.redis_pool.with do |conn|
                SidekiqWeb.cleanup_job_progress_state_logs(
                  conn,
                  day,
                  job_name,
                  queue_name,
                  uniqueness_id
                )
              end

              redirect URI(request.referer).path
            end

            # delete single uniqueness log
            app.post '/job_stats/logs/:day/:job_name/:queue_name/:uniqueness_id/:job_id/delete' do
              job_name = route_params[:job_name]
              queue_name = route_params[:queue_name]
              day = route_params[:day].to_i
              uniqueness_id = route_params[:uniqueness_id].to_s
              job_id = route_params[:job_id]

              Sidekiq.redis_pool.with do |conn|
                SidekiqWeb.cleanup_job_progress_state_log_one(
                  conn,
                  day,
                  job_name,
                  queue_name,
                  uniqueness_id,
                  job_id
                )
              end

              redirect URI(request.referer).path
            end

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
