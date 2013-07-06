module Wyrm
  # This bypasses the need to marshal objects
  # between two pumps. io will have to be some
  # kind of queue?
  # Could hook up to a fifo, but that would still require marshalling.
  # io must respond_to eof? for DumpSchema to work
  class Hole
    def initialize( other_pump )
      @other_pump = other_pump
    end

    def encode( obj, io )
      # write obj to other_pump
    end

    def decode( io, &block )
      obj = # read from other_pump
      yield obj if block_given?
      obj
    end
  end
end
