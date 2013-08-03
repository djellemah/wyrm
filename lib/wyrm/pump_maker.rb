require 'wyrm/db_pump'

class Object
  def call_or_self( maybe_callable )
    if maybe_callable.respond_to? :call
      maybe_callable.call( self )
    else
      maybe_callable
    end
  end
end

module PumpMaker
  def make_pump( db, pump_thing )
    call_or_self(pump_thing) || DbPump.new( db, nil )
  end

  def maybe_deebe( db_or_string )
    case db_or_string
    when String
      Sequel.connect db_or_string
    when Sequel::Database
      db_or_string
    else
      raise "Don't know how to db-ify #{db_or_string.inspect}"
    end
  end
end
