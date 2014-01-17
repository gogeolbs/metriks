require 'moped'

module Metriks::Reporter
  class Mongo
    attr_accessor :prefix

    def initialize(options={})

      @host = options[:host]
      @port = options[:port] || 27017
      @prefix = "metriks"
      @database = options[:database] || 'metriks'
      @document = options[:document]
      @interval = options[:interval] || 60
      @registry  = options[:registry] || Metriks::Registry.default
      @on_error  = options[:on_error] || proc { |ex| }

      initialize_mongo

    end

    def initialize_mongo
      begin
        @session = Moped::Session.new([ @host.to_s + ":" + @port.to_s ])
        @session.use @database
      rescue Exception => e
        puts "error", e
      end
    end

    def start
      @thread ||= Thread.new do
        loop do
          sleep @interval
          Thread.new do
            begin
              write
            rescue Exception => ex
              @on_error[ex] rescue nil
            end
          end
        end
      end
    end

    def stop
      @thread.kill if @thread
      @thread = nil
    end

    def restart
      stop
      start
    end

    def write
      @registry.each do |name, metric|
        case metric
        when Metriks::Meter
          send_metric name, metric, [
            :count,
            :one_minute_rate,
            :five_minute_rate,
            :fifteen_minute_rate,
            :mean_rate
          ]
        when Metriks::Counter
          send_metric name, metric, [
            :count
          ]
          metric.clear if metric.reset_on_submit
        when Metriks::Gauge
          send_metric name, metric, [
            :value
          ]
        when Metriks::UtilizationTimer
          send_metric name, metric, [
            :count, :one_minute_rate, :five_minute_rate,
            :fifteen_minute_rate, :mean_rate,
            :min, :max, :mean, :stddev,
            :one_minute_utilization, :five_minute_utilization,
            :fifteen_minute_utilization, :mean_utilization,
          ], [
            :median, :get_95th_percentile
          ]
        when Metriks::Timer
          send_metric name, metric, [
            :count, :one_minute_rate, :five_minute_rate,
            :fifteen_minute_rate, :mean_rate,
            :min, :max, :mean, :stddev
          ], [
            :median, :get_95th_percentile
          ]
        when Metriks::Histogram
          send_metric name, metric, [
            :count, :min, :max, :mean, :stddev
          ], [
            :median, :get_95th_percentile
          ]
        end
      end
    end

    def send_metric(compound_name, metric, keys, snapshot_keys = [])
      array = compound_name.split("#")

      if array.length == 2
        username, metric_id = array
      elsif array.length == 3
        username, metric_id, database = array
      elsif array.length == 4
        username, metric_id, database, cname = array
      else
        return
      end

      keys.each do |key|
        options = {
          username: username,
          metric_id: metric_id + "." + key.to_s,
          timestamp: DateTime.now.to_time,
          value: metric.send(key)
        }

        if database
          options[:database] = database
        end

        if cname
          options[:cname] = cname
        end

        can_store = true

        if metric.respond_to?(:store_zero_value) && !metric.store_zero_value
          can_store = options[:value] > 0
        end

        if can_store
          @session.with(safe: true) do |safe|
            safe[@document].insert(options)
          end
        end
      end 
    end # end send_metric
  end # end class
end # end module