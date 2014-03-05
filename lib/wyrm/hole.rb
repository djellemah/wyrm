require 'thread'
require 'logger'

require 'wyrm/db_pump'
require 'wyrm/pump_maker'
require 'wyrm/schema_migrations'

class Method
  def kwargs_as_hash( invocation_binding )
    named_locals = parameters. \
      select{|type,_| type == :key}. \
      flat_map{|_,name| [name,invocation_binding.eval(name.to_s)]}

    Hash[ *named_locals ]
  end
end

module Wyrm
  # This bypasses the need to marshal objects between two pumps.
  # It uses a queue of the record arrays instead.
  class Hole
    include PumpMaker

    # This is the codec. Named for the mouth of a wormhole. Cos finding a good name for this is hard.
    #
    # Connects the two db_pumps together. Implements Codec, Quacks like IO.
    class Mouth
      def encode( obj, io_queue )
        io_queue.enq obj
      end

      def decode( io_queue, &block )
        obj = io_queue.deq
        yield obj if block_given?
        obj
      end

      def reset
        @flushed = false
        queue.clear
      end

      # queue could be empty while producer is generating something,
      # so only eof after flushed has been called.
      def eof?
        @flushed && queue.empty?
      end

      # this gets called after dump is finished, by db_pump
      def flush
        @flushed = true
      end

      def queue
        @queue ||=
        if RUBY_VERSION == '2.1.0'
          logger.notice "SizedQueue broken in 2.1.0 (https://bugs.ruby-lang.org/issues/9302). Falling back to Queue, which may run out of memory."
          Queue.new
        else
          SizedQueue.new 5000
        end
      end

      def enq( *args )
        queue.enq( *args )
      end

      def deq( *args )
        queue.deq( *args )
      end
    end

    def logger
      @logger ||= Logger.new( STDERR ).tap do |lgr|
        lgr.level = Logger::INFO
      end
    end

    def initialize( src_db, dst_db, drop_tables: true, queue_size: 5000 )
      # called only once per run, so not really a performance issue
      @options = method(__method__).kwargs_as_hash( binding )

      @src_db = maybe_deebe src_db
      @dst_db = maybe_deebe dst_db

      @src_db.extension :schema_dumper
    end

    attr_reader :src_db, :dst_db, :options

    def mouth
      @mouth ||= Mouth.new
    end

    def src_pump
      @src_pump ||= DbPump.new( {db: src_db, io: mouth, codec: mouth, logger: logger}.merge( options[:db_pump] ||{} ) )
    end

    def dst_pump
      @dst_pump ||= DbPump.new( {db: dst_db, io: mouth, codec: mouth, logger: logger}.merge( options[:db_pump] ||{} ) )
    end

    def transfer_table( table_name )
      mouth.reset
      src_pump.table_name = dst_pump.table_name = table_name

      if src_pump.table_dataset.empty?
        logger.info "No records in #{table_name}"
        return
      end

      threading = false
      if threading
        # Use threads so the db read/writes aren't waiting for
        # one another.
        send_thread = Thread.new{ src_pump.dump }
        recv_thread = Thread.new{ dst_pump.restore }

        send_thread.join
        recv_thread.join
      else
        src_pump.dump
        dst_pump.restore
      end
    end

    # needed by SchemaMigrations
    def same_db
      src_db == dst_db
    end

    include SchemaMigrations

    def transfer_schema( &transfer_table_block )
      create_tables

      # transfer tables here
      yield self if block_given?

      # TODO duplicate of RestoreSchema.index
      # create indexes and foreign keys, and reset sequences
      logger.info "creating indexes"
      eval( index_migration ).apply dst_db, :up

      logger.info "creating foreign keys"
      eval( fk_migration ).apply dst_db, :up

      if dst_db.database_type == :postgres
        logger.info "reset primary key sequences"
        dst_db.tables.each{|t| dst_db.reset_primary_key_sequence(t)}
        logger.info "Primary key sequences reset successfully"
      end
    end

    def transfer_tables
      logger.info "transferring tables"
      src_db.tables.each do |table_name|
        transfer_table table_name
      end
    end

    def transfer
      if options[:drop_tables]
        logger.info "dropping tables"
        drop_tables src_db.tables
      end

      transfer_schema do
        transfer_tables
      end
    end

  end
end
