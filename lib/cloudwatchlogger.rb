require File.join(File.dirname(__FILE__), 'cloudwatchlogger', 'client')

require 'logger'
require "active_support/logger_silence"

module CloudWatchLogger
  class LogGroupNameRequired < ArgumentError; end
  class LogEventRejected < ArgumentError; end



  def self.new(credentials, log_group_name, log_stream_name = nil, opts = {})
    client = CloudWatchLogger::Client.new(credentials, log_group_name, log_stream_name, opts)
    logger = Logger.new(client)
    logger.class.include ActiveSupport::LoggerSilence
    class << logger
      attr_accessor :local_level
    end
    if client.respond_to?(:formatter)
      logger.formatter = client.formatter(opts[:format])
    elsif client.respond_to?(:datetime_format)
      logger.datetime_format = client.datetime_format
    end

    logger
  end
end
