require 'logger'
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

# Dump a schema and compressed data from a db to a set of files
#  src_db = Sequel.connect "postgres://localhost:5454/lots"
#  ds = DumpSchema.new src_db, Pathname('/var/data/lots')
#  ds.dump_schema
#  ds.dump_tables
class DumpSchema
  def initialize( src_db, container = nil, pump: nil )
    @src_db = src_db
    @container = Pathname(container)
    @pump = make_pump( pump )
  end

  attr_reader :src_db, :container, :pump

  def make_pump( pump_thing )
    call_or_self(pump_thing) || DbPump.new( src_db, nil )
  end

  def schema_migration
    @schema_migration ||= src_db.dump_schema_migration(:indexes=>false, :same_db => same_db)
  end

  def index_migration
    @index_migration ||= src_db.dump_indexes_migration(:same_db => same_db)
  end

  def fk_migration
    @fk_migration ||= src_db.dump_foreign_key_migration(:same_db => same_db)
  end

  def restore_migration
    <<-EOF
      require 'restore_migration'
      Sequel.migration do
        def db_pump
        end

        up do
          restore_tables
        end

        down do
          # from each table clear table
          each_table do |table_name|
            db_pump.restore table_name, io: io, db: db
          end
        end
      end
    EOF
  end

  def same_db
    false
  end

  def logger
    @logger ||= Logger.new STDERR
  end

  def dump_schema
    (container + '001_schema.rb').open('w') do |io|
      io.write schema_migration
    end

    (container + '002_populate_tables.rb').open('w') do |io|
      io.write restore_migration
    end

    (container + '003_indexes.rb').open('w') do |io|
      io.write index_migration
    end

    (container + '004_foreign keys.rb').open('w') do |io|
      io.write fk_migration
    end
  end

  def open_bz2( pathname )
    fio = pathname.open('w')
    # open subprocess in read-write mode
    zio = IO.popen( "pbzip2 -z", 'r+' )
    copier = Thread.new do
      begin
        IO.copy_stream zio, fio
        logger.debug "finished stream copy"
      ensure
        fio.close
      end
    end

    yield zio

    # signal the copier thread to stop
    zio.close_write
    logger.debug 'finished dumping'
    # wait for copier thread to
    copier.join
    logger.debug 'stream copy thread finished'
  ensure
    zio.close unless zio.closed?
    fio.close unless fio.closed?
  end

  def dump_tables
    src_db.tables.each do |table_name|
      filename = container + "#{table_name}.dbp.bz2"
      logger.info "dumping #{table_name} to #{filename}"
      open_bz2 filename do |zio|
        # generate the dump
        pump.table_name = table_name
        pump.io = zio
        pump.dump
      end
    end
  end
end
