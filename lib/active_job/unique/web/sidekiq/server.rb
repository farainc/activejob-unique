module ActiveJob
  module Unique
    module Web
      module SidekiqWeb
        module Server
          def self.registered(app)
            # index page of stats
            app.get '/job_stats' do
              redirect '/job_stats/*/*'
            end

            app.get '/job_stats/:job_prefix/:queue_name' do
              view_path = File.join(File.expand_path(__dir__), 'views')

              @today = SidekiqWeb.sequence_today

              @job_prefix = route_params[:job_prefix].to_s
              @queue_name = route_params[:queue_name].to_s

              @job_stats = []
              @job_stats_today = {}
              @job_stats_all_time = {}
              @job_log_keys = {}
              @uniqueness_flag_keys = {}
              @count = (params[:count] || 10).to_i
              @current_page = params[:page].to_i
              @current_page = 1 if @current_page < 1

              begin_index = (@current_page - 1) * @count
              next_page_availabe = false

              Sidekiq.redis_pool.with do |conn|
                if @job_prefix == '*' && @queue_name == '*'
                  @total_size = conn.zcount(SidekiqWeb.job_progress_stats_jobs, '-inf', '+inf')
                  @job_stats = conn.zrevrange(SidekiqWeb.job_progress_stats_jobs, begin_index, begin_index + @count - 1)

                  SidekiqWeb.cleanup_yesterday_progress_stats(conn, Time.now.utc)
                else
                  @job_stats = conn.zrevrange(SidekiqWeb.job_progress_stats_jobs, 0, -1)
                  @job_stats.reject!{|job| (job =~ /^#{@job_prefix}/i) != 0 } if @job_prefix != '*'

                  @total_size = @job_stats.size
                end
              end

              Sidekiq.redis_pool.with do |conn|
                next_page_availabe, @job_stats_today = SidekiqWeb.regroup_job_progress_stats_today(conn, @job_stats, @queue_name, @count)
                next_page_availabe, @job_stats_all_time = SidekiqWeb.regroup_job_progress_stats(conn, @job_stats, @queue_name, @count)

                @uniqueness_flag_keys = SidekiqWeb.group_job_progress_stage_uniqueness_flag_keys(conn, @job_stats_all_time.keys)
                @processing_flag_keys = SidekiqWeb.group_job_progress_stage_processing_flag_keys(conn, @job_stats_all_time.keys)

                @job_log_keys = SidekiqWeb.group_job_progress_stage_log_keys(conn, @job_stats_all_time)

                @job_stats.reject!{|job| !@job_stats_all_time.key?(job) } unless @job_prefix == '*' && @queue_name == '*'
              end

              if @queue_name != '*'
                @total_size = @count * (@current_page - 1) + @job_stats.size
                @total_size += 1 if next_page_availabe
              end

              render(:erb, File.read(File.join(view_path, 'index.erb')))
            end

            app.get '/job_stats/uniqueness/:job_name/:queue_name/:stage' do
              view_path = File.join(File.expand_path(__dir__), 'views')

              @job_name = route_params[:job_name]
              @queue_name = route_params[:queue_name]
              @stage = route_params[:stage]
              @job_stats = []

              @count = (params[:count] || 25).to_i
              @current_page = params[:page].to_i
              @current_page = 1 if @current_page < 1

              begin_index = (@current_page - 1) * @count
              next_page_availabe = false

              Sidekiq.redis_pool.with do |conn|
                next_page_availabe, @job_stats = SidekiqWeb.query_job_progress_stage_state_uniqueness(
                  conn,
                  @job_name,
                  @queue_name,
                  @stage,
                  @count,
                  begin_index
                )
              end

              @total_size = @count * (@current_page - 1) + @job_stats.size
              @total_size += 1 if next_page_availabe

              render(:erb, File.read(File.join(view_path, 'uniqueness.erb')))
            end

            # delete single uniqueness flag
            app.post '/job_stats/uniqueness/:job_name/:queue_name/:stage/:uniqueness_id/delete' do
              job_name = route_params[:job_name]
              queue_name = route_params[:queue_name]
              uniqueness_id = route_params[:uniqueness_id].to_s
              stage = route_params[:stage]

              Sidekiq.redis_pool.with do |conn|
                SidekiqWeb.cleanup_job_progress_state_uniqueness(
                  conn,
                  job_name,
                  queue_name,
                  stage,
                  uniqueness_id
                )
              end

              redirect URI(request.referer).path
            end

            app.get '/job_stats/processing/:job_name/:queue_name/:uniqueness_id' do
              view_path = File.join(File.expand_path(__dir__), 'views')

              @job_name = route_params[:job_name]
              @queue_name = route_params[:queue_name]
              @uniqueness_id = route_params[:uniqueness_id]
              @job_stats = []

              @count = (params[:count] || 25).to_i
              @current_page = params[:page].to_i
              @current_page = 1 if @current_page < 1

              begin_index = (@current_page - 1) * @count
              next_page_availabe = false

              Sidekiq.redis_pool.with do |conn|
                next_page_availabe, @job_stats = SidekiqWeb.query_job_progress_stage_state_processing(
                  conn,
                  @job_name,
                  @queue_name,
                  @uniqueness_id,
                  @count,
                  begin_index
                )
              end

              @total_size = @count * (@current_page - 1) + @job_stats.size
              @total_size += 1 if next_page_availabe

              render(:erb, File.read(File.join(view_path, 'processing.erb')))
            end

            # delete single processing flag
            app.post '/job_stats/processing/:job_name/:queue_name/:uniqueness_id/:stage/delete' do
              job_name = route_params[:job_name]
              queue_name = route_params[:queue_name]
              uniqueness_id = route_params[:uniqueness_id].to_s
              stage = route_params[:stage].to_s

              Sidekiq.redis_pool.with do |conn|
                SidekiqWeb.cleanup_job_progress_state_processing(
                  conn,
                  job_name,
                  queue_name,
                  uniqueness_id,
                  stage
                )
              end

              redirect URI(request.referer).path
            end

            app.get '/job_stats/logs/:day/:job_name/:queue_name/:uniqueness_id' do
              view_path = File.join(File.expand_path(__dir__), 'views')

              @job_logs = []
              @job_name = route_params[:job_name]
              @queue_name = route_params[:queue_name].to_s
              @day = route_params[:day].to_i
              @day = SidekiqWeb.sequence_today if @day < SidekiqWeb.sequence_day(Time.now.utc - 3600 * 24 * 9)

              @uniqueness_id = route_params[:uniqueness_id].to_s
              @uniqueness_id = '*' unless @uniqueness_id.size == 32

              @count = (params[:count] || 25).to_i
              @current_page = params[:page].to_i
              @current_page = 1 if @current_page < 1

              begin_index = (@current_page - 1) * @count
              next_page_availabe = false

              Sidekiq.redis_pool.with do |conn|
                next_page_availabe, @job_logs = SidekiqWeb.query_job_progress_stage_log_jobs(
                  conn,
                  @day,
                  @job_name,
                  @queue_name,
                  @uniqueness_id,
                  @count,
                  begin_index
                )

                # when first page display, always clean logs data 8 days ago
                if @current_page == 1 && @queue_name == '*' && @uniqueness_id == '*'
                  SidekiqWeb.cleanup_job_progress_stage_logs(conn, SidekiqWeb.sequence_day(Time.now.utc - 3600 * 24 * 7), @job_name, @queue_name, @uniqueness_id)
                end
              end

              @total_size = @count * (@current_page - 1) + @job_logs.size
              @total_size += 1 if next_page_availabe

              render(:erb, File.read(File.join(view_path, 'logs.erb')))
            end

            app.get '/job_stats/logs/:day/:job_name/:queue_name/:uniqueness_id/:job_id' do
              view_path = File.join(File.expand_path(__dir__), 'views')

              @job_log = {}
              @job_name = route_params[:job_name]
              @queue_name = route_params[:queue_name]
              @day = route_params[:day].to_i
              @uniqueness_id = route_params[:uniqueness_id].to_s
              @job_id = route_params[:job_id]

              Sidekiq.redis_pool.with do |conn|
                @job_log = SidekiqWeb.query_job_progress_stage_log_job_one(
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
                SidekiqWeb.cleanup_job_progress_stage_logs(
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
