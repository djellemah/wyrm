require 'wyrm/pump'
require 'wyrm/module'

module Wyrm::PumpMaker
  def call_or_self( maybe_callable )
    if maybe_callable.respond_to? :call
      maybe_callable.call( self )
    else
      maybe_callable
    end
  end

  def make_pump( db, pump_thing )
    call_or_self(pump_thing) || Pump.new( db: db )
  end

  def maybe_deebe( db_or_string )
    case db_or_string
    when String
      begin
        Sequel.connect db_or_string
      rescue Sequel::AdapterNotFound
        puts "\nCan't find db driver for #{db_or_string}. It might work to do\n\n  gem install #{db_or_string.split(?:).first}\n\n"
        exit(1)
      end
    when Sequel::Database
      db_or_string
    else
      raise "Don't know how to db-ify #{db_or_string.inspect}"
    end
  end
end
