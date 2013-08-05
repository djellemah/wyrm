require 'logger'
require 'wyrm/pump_maker'

# Dump a schema and compressed data from a db to a set of files
#  src_db = Sequel.connect "postgres://localhost:5454/lots"
#  ds = DumpSchema.new src_db, Pathname('/var/data/lots')
#  ds.dump_schema
#  ds.dump_tables
class DumpSchema
  include PumpMaker

  def initialize( src_db, container = nil, pump: nil )
    @src_db = maybe_deebe src_db
    @container = Pathname.new container
    @pump = make_pump( @src_db, pump )

    @src_db.extension :schema_dumper
  end

  attr_reader :src_db, :container, :pump

  def schema_migration
    @schema_migration ||= src_db.dump_schema_migration(:indexes=>false, :same_db => same_db)
  end

  def index_migration
    @index_migration ||= src_db.dump_indexes_migration(:same_db => same_db)
  end

  def fk_migration
    @fk_migration ||= src_db.dump_foreign_key_migration(:same_db => same_db)
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

    (container + '002_indexes.rb').open('w') do |io|
      io.write index_migration
    end

    (container + '003_foreign_keys.rb').open('w') do |io|
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

    # wait for copier thread to finish
    copier.join
    logger.debug 'stream copy thread finished'
  ensure
    zio.close unless zio.closed?
    fio.close unless fio.closed?
  end

  def dump_table( table_name, &io_block )
    pump.table_name = table_name
    if pump.table_dataset.empty?
      logger.info "No records in #{table_name}"
      return
    end

    filename = container + "#{table_name}.dbp.bz2"
    logger.info "dumping #{table_name} to #{filename}"

    open_bz2 filename do |zio|
      # generate the dump
      pump.io = zio
      pump.dump
    end
  rescue
    logger.error "failed dumping #{table_name}: #{$!.message}"
  end

  def dump_tables
    src_db.tables.each do |table_name|
      dump_table table_name
    end
  end
end
