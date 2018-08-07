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
                @state_keys = SidekiqWeb.job_progress_state_uniqueness_keys(conn, job_names)

                @job_stats_all_time = SidekiqWeb.regroup_job_progress_stats(SidekiqWeb.job_progress_stats, job_names, conn)
                @job_stats_today = SidekiqWeb.regroup_job_progress_stats("#{SidekiqWeb.job_progress_stats}:#{today}", job_names, conn)

                @job_log_keys = SidekiqWeb.job_progress_state_log_keys(conn, @job_stats_all_time)

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

            app.get '/job_stats/logs/:job_name/:queue_name' do
              view_path = File.join(File.expand_path('..', __FILE__), 'views')

              @job_stats = []
              @job_name = route_params[:job_name]
              @queue_name = route_params[:queue_name]
              @day = params[:day] || SidekiqWeb.sequence_today


              @count = (params[:count] || 100).to_i
              @current_page = params[:page].to_i
              @current_page = 1 if @current_page < 1

              begin_index = (@current_page - 1) * @count
              next_page_availabe = false

              Sidekiq.redis_pool.with do |conn|
                next_page_availabe, @job_stats = SidekiqWeb.job_progress_state_log_uniqueness_ids(conn, @day, @job_name, @queue_name, @count, begin_index)
              end

              @total_size = @count * (@current_page - 1) + @job_stats.size
              @total_size += 1 if next_page_availabe

              render(:erb, File.read(File.join(view_path, 'logs.erb')))
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
