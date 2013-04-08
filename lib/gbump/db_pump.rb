require 'sequel'
require 'yaml'
require 'ostruct'
require 'logger'
require 'fastandand'

Sequel.extension :migration, :schema_dumper, :pagination

# TODO possibly use Gem::Package::TarWriter to write tar files
# TODO when restoring, could use a SizeQueue to make sure the db is kept busy

class DbPump
  class Respondent
    def initialize( *methods )
      @methods = methods
    end

    def ===( instance )
      @methods.all?{|m| instance.respond_to? m}
    end
  end

  def initialize( codec = :marshal )
    @codec =
    case codec
    when :yaml; YamlCodec.new
    when :marshal; MarshalCodec.new
    when Class
      codec.new
    when Respondent.new( :encode, :decode )
      codec
    else
      raise "unknown codec #{codec}"
    end
  end

  attr_reader :codec

  # TODO could use msgpack as serialization here, but its API is unpleasant.

  class MarshalCodec
    def encode( obj, io )
      Marshal.dump obj, io
    end

    def decode( io, &block )
      obj = Marshal.load(io)
      yield obj if block_given?
      obj
    end
  end

  class MsgPackCodec
    def encode( obj, io )
      Marshal.dump obj, io
    end

    def decode( io, &block )
      obj = Marshal.load(io)
      yield obj if block_given?
      obj
    end
  end

  class YamlCodec
    def encode( obj, io )
      YAML.dump obj, io
    end

    def decode( io, &block )
      obj = YAML.load(io)
      yield obj if block_given?
      obj
    end
  end

  def logger
    @logger ||= Logger.new STDERR
  end

  def primary_keys( db, table_name )
    db.schema(table_name).select{|df| df.last[:primary_key]}.map{|df| df.first}
  end

  # TODO possibly use select from outer / inner join to
  # http://www.numerati.com/2012/06/26/reading-large-result-sets-with-hibernate-and-mysql/
  # because mysql is useless
  def paginated_dump( table_name, options = {} )
    options = OpenStruct.new( {io: STDOUT, page_size: 10000, dry_run: false}.merge( options.to_h ) )
    pk = primary_keys options.db, table_name
    options.db[table_name].order(*pk).each_page(options[:page_size]) do |page|
      logger.info page.sql
      page.each do |row|
        unless options[:dry_run]
          codec.encode row.values, options.io
        end
      end
    end
    options.io.flush
  end

  # have to use this for non-integer pks
  # The idea is that large offsets are expensive in the db because the db server has to read
  # through the data set to reach the required offset. So make that only ids, and then
  # do the main select from the limited id list.
  # TODO could speed this up by have a query thread which runs the next page-query while
  # the current one is being written/compressed.
  # select * from massive as full
  #   inner join (select id from massive order by whatever limit m, n) limit
  #   on full.id = limit.id
  # order by full.whatever
  def inner_dump( table_name, options = {} )
    options = OpenStruct.new( {io: STDOUT, page_size: 10000, dry_run: false}.merge( options.to_h ) )
    pk = primary_keys options.db, table_name

    table_dataset = options.db[table_name]
    # could possibly overrride Dataset#paginate(page_no, page_size, record_count=nil)
    0.step(table_dataset.count, options.page_size).each do |offset|
      limit_dataset = table_dataset.select( *pk ).limit( options.page_size, offset ).order( *pk )
      page = table_dataset.join( limit_dataset, Hash[ pk.map{|f| [f,f]} ] ).order( *pk ).qualify_to(table_name)
      logger.info page.sql
      page.each do |row|
        unless options[:dry_run]
          codec.encode row.values, options.io
        end
      end
    end
    options.io.flush
  end

  # need to also dump a first row containing useful stuff:
  # - source table name
  # - number of rows
  # - source db url
  # - permissions?
  # These should all be in one object that can be Marshall.load-ed easily.
  def dump( table_name, options = {} )
    pk = primary_keys options[:db], table_name
    case
    when pk.empty?
      paginated_dump( table_name, options )
    when pk.all?{|i| i == :id }
      min_max_dump( table_name, options )
    else
      inner_dump( table_name, options )
    end
  end

  # could use this for integer pks
  def min_max_dump( table_name, options = {} )
    # select max(id), min(id) from patents
    # and then split that up into 10000 size chunks. Not really important if there aren't exactly 10000
    options = OpenStruct.new( {io: STDOUT, page_size: 10000, dry_run: false}.merge( options.to_h ) )
    pk = primary_keys options.db, table_name

    table_dataset = options.db[table_name]
    min, max = table_dataset.select{[min(id), max(id)]}.first.values
    # could possibly overrride Dataset#paginate(page_no, page_size, record_count=nil)
    # TODO definitely need to refactor this

    # will always include the last item because
    (min..max).step(options.page_size).each do |offset|
      page = table_dataset.where( id: offset...(offset+options.page_size) ).order( *pk )
      logger.info page.sql
      page.each do |row|
        unless options[:dry_run]
          codec.encode row.values, options.io
        end
      end
    end
    options.io.flush
  end

  # TODO possible memory issues here if the rows are big. May need to fork this.
  def restore( table_name, options = {} )
    options = OpenStruct.new( {io: STDIN, page_size: 10000, start_row: 0, dry_run: false}.merge( options ) )
    dataset = options.db[table_name.to_sym]
    # destination db should be same structure as incoming data
    column_names = options.db.schema(table_name.to_sym).map( &:first )
    first = ->(row){raise "schema mismatch" if row.size != column_names.size}

    rows_restored = 0

    # skip this many rows
    options.start_row.times do
      codec.decode( options.io ) {|row|}
    end

    # copy rows into db
    while !options.io.eof?
      # fetch a page of rows
      rows_ary = []
      begin
        options.page_size.times do |i|
          codec.decode( options.io ) do |row|
            rows_ary << row
          end
          rows_restored += 1
        end
      rescue EOFError => e
        # ran out of rows, so just use the ones we have so far
      end

      # insert to db. Hopeful db support bulk insert, which Sequel will figure out
      options.db.transaction do
        dataset.import column_names, rows_ary
        yield rows_restored if block_given?
        logger.info "restored #{rows_restored}"
      end
    end

    rows_restored
  end

  def from_bz2( filename, db, table_name, options = {} )
    IO.popen( "pbzip2 -d -c #{filename}" ) do |io|
      restore table_name, options.merge( io: io, db: db )
    end
  end
end

# There are actually 2 sources for this:
# one is the src db, the other is the dumped files
# And the one that transfers live is another version
class Schema
  def initialize( src_db, dst_db = nil )
    @src_db = src_db
    @dst_db = dst_db
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

  attr_accessor :dst_db
  attr_reader :src_db

  def same_db
    @dst_db.andand.database_type == @src_db.andand.database_type
  end

  def logger
    @logger ||= Logger.new STDERR
  end

  # create the destination schema
  def create
    eval( @schema_migration ).apply dst_db, :up
  end

  # create indexes and foreign keys, and reset sequences
  def index
    logger.info "creating indexes"
    eval(@index_migration).apply dst, :up
    logger.info "creating foreign keys"
    eval(@fk_migration).apply dst, :up

    if dst.database_type == :postgres
      logger.info "reset primary key sequences"
      dst.tables.each{|t| dst.reset_primary_key_sequence(t)}
      logger.info "Primary key sequences reset successfully"
    end
  end

  def transfer_table( table_name, options = {} )
    options = OpenStruct.new( {page_size: 10000, dry_run: false}.merge( options ) )
    total_records = @src_db[table_name].count
    logger.info "transferring #{total_records}"
    column_names = @src_db.schema(table_name.to_sym).map( &:first )

    @src_db[table_name].each_page(options.page_size) do |page|
      logger.info "#{page.sql} of #{total_records}"
      unless options.dry_run
        @dst_db.transaction do
          rows_ary = []
          page.each do |row_hash|
            rows_ary << row_hash.values
          end
          @dst_db[table_name.to_sym].import column_names, rows_ary
        end
      end
    end
  end

  # copy the data in the tables
  def transfer
    create
    transfer_tables
    index
  end

  def dump_schema( container, options = {codec: :marshal} )
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

  def load_migrations( container )
    @schema_migration = eval (container + '001_schema.rb').read
    @index_migration = eval (container + '003_indexes.rb').read
    @fk_migration = eval (container + '004_foreign keys.rb').read
  end

  def dump_one_table( table_name, pathname, db_pump )
    logger.info "dumping #{table_name} to #{pathname}"
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

    # generate the dump
    db_pump.dump table_name, db: src_db, io: zio

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

  def dump_tables( container, options = {:codec => :marshal} )
    container = Pathname(container)
    db_pump = DbPump.new( options[:codec] )

    src_db.tables.each do |table_name|
      filename = container + "#{table_name}.dbp.bz2"
      dump_one_table table_name, filename, db_pump
    end
  end

  def restore_one_table( table_file, db_pump )
    logger.info "restoring from #{table_file}"
    table_name = table_file.basename.sub_ext('').sub_ext('').to_s.to_sym
    # check if table has been restored already, and has the correct rows,
    # otherwise pass in a start row.
    db_pump.from_bz2 table_file, dst_db, table_name
  end

  def restore_tables( container, options = {:codec => :marshal} )
    db_pump = DbPump.new( options[:codec] )
    table_files = Pathname.glob Pathname(container) + '*dbp.bz2'
    table_files.each{|table_file| restore_one_table table_file, db_pump}
  end

  def restore_tables( container, options = {:codec => :marshal} )
    container = Pathname(container)
    container.child ren
  end

  def self.transfer( src_db, dst_db )
    new( src_db, dst_db ).transfer
  end
end
