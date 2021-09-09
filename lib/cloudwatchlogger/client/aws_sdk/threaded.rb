require 'aws-sdk-cloudwatchlogs'
require 'thread'

module CloudWatchLogger
  module Client
    class AWS_SDK
      # Used by the Threaded client to manage the delivery thread
      # recreating it if is lost due to a fork.
      #
      class DeliveryThreadManager
        def initialize(credentials, log_group_name, log_stream_name, opts = {})
          @credentials = credentials
          @log_group_name = log_group_name
          @log_stream_name = log_stream_name
          @opts = opts
          start_thread
        end

        # Pushes a message to the delivery thread, starting one if necessary
        def deliver(message)
          start_thread unless @thread.alive?
          @thread.deliver(message)
          # Race condition? Sometimes we need to rescue this and start a new thread
        rescue NoMethodError
          @thread&.kill # Try not to leak threads, should already be dead anyway
          start_thread
          retry
        end

        private

        def start_thread
          @thread = DeliveryThread.new(@credentials, @log_group_name, @log_stream_name, @opts)
        end
      end

      class DeliveryThread < Thread
        def initialize(credentials, log_group_name, log_stream_name, opts = {})
          opts[:open_timeout] = opts[:open_timeout] || 120
          opts[:read_timeout] = opts[:read_timeout] || 120
          @credentials = credentials
          @log_group_name = log_group_name
          @log_stream_name = log_stream_name
          @opts = opts 
          @events = []
          @sent_at = Time.now
          @queue = Queue.new
          @exiting = false

          super do
            loop do
              connect!(opts) if @client.nil?
              send_events if should_send?
              begin
                message_object = @queue.pop(true)
              rescue ThreadError
                message_object = nil
              end
              break if message_object == :__delivery_thread_exit_signal__
              
              if message_object
                send_events if should_send? message_object[:message].bytesize
                add_event message_object
              end
              # we not longer suspend when the queue is empty, so we must sleep
              sleep 0.5

            end
          end

          at_exit do
            exit!
            join
          end
        end 

        def should_send? new_message_size = 0
          return true if event_queue_size + new_message_size > 1048000  
          return true if @sent_at < Time.now - 5.seconds && @events.count > 0
          return true if @events.count > 5000
        end

        def event_queue_size
          @events.sum { |event| event.message.bytesize }
        end

           
        def add_event message_object
          event = {
            timestamp: message_object[:epoch_time],
            message:   message_object[:message].slice(0..948000)
          }
          @events.push event
        end

        def send_events
          payload = {
            log_group_name: @log_group_name,
            log_stream_name: @log_stream_name,
            log_events: @events
          }
          begin
            payload[:sequence_token] = @sequence_token if @sequence_token            
            response = @client.put_log_events(payload)
          rescue Aws::CloudWatchLogs::Errors::InvalidSequenceTokenException => err
            @sequence_token = err.message.split(' ').last
            retry
          end

          unless response.rejected_log_events_info.nil?
            raise CloudWatchLogger::LogEventRejected
          end

          @sent_at = Time.now
          @events = []
          @sequence_token = response.next_sequence_token
        end

        # Signals the queue that we're exiting
        def exit!
          @exiting = true
          @queue.push :__delivery_thread_exit_signal__
        end

        # Pushes a message onto the internal queue
        def deliver(message)
          @queue.push(message)
        end

        def connect!(opts = {})
          args = { http_open_timeout: opts[:open_timeout], http_read_timeout: opts[:read_timeout] }
          args[:region] = @opts[:region] if @opts[:region]
          args.merge!( @credentials.key?(:access_key_id) ? { credentials: Aws::Credentials.new(@credentials[:access_key_id], @credentials[:secret_access_key], @credentials[:session_token] )} : { credentials: Credentials.new()} )
          @client = Aws::CloudWatchLogs::Client.new(args)
          begin
            @client.create_log_stream(
              log_group_name: @log_group_name,
              log_stream_name: @log_stream_name
            )
          rescue Aws::CloudWatchLogs::Errors::ResourceNotFoundException
            @client.create_log_group(
              log_group_name: @log_group_name
            )
            retry
          rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException, 
            Aws::CloudWatchLogs::Errors::AccessDeniedException
          end
        end
      end
    end
  end
end
