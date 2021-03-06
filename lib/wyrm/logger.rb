require 'logger'

module Wyrm
  module Logger
    def logger
      @logger ||= ::Logger.new( STDERR ).tap do |lgr|
        lgr.level = ::Logger::DEBUG
      end
    end
  end
end
