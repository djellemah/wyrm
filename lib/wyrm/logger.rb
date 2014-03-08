require 'logger'

module Wyrm
  module Logger
    def logger
      @logger ||= ::Logger.new( STDERR ).tap do |lgr|
        lgr.level = ::Logger::INFO
      end
    end
  end
end
