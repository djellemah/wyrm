module Wyrm
  # This bypasses the need to marshal objects
  # between two pumps. io will have to be some
  # kind of queue?
  # Could hook up to a fifo, but that would still require marshalling.
  # io must respond_to eof? for DumpSchema to work
  class Hole
    include PumpMaker

    def initialize( src_db, dst_db, pump: nil )
      @src_db = maybe_deebe src_db
      @dst_db = maybe_deebe dst_db
      @pump = make_pump( @src_db, pump )

      @src_db.extension :schema_dumper
    end

    attr_reader :src_db, :dst_db, :pump

    # This is the codec.
    # From the mouth of a wormhole
    class Mouth
      def encode( obj, queue )
        queue.push obj
      end

      def decode( queue, &block )
        obj = queue.pop
        yield obj if block_given?
        obj
      end
    end

  end
end
