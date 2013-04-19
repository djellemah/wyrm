require 'sequel'
require 'yaml'
require 'ostruct'
require 'logger'
require 'fastandand'

Sequel.extension :migration, :schema_dumper, :pagination

# TODO possibly use Gem::Package::TarWriter to write tar files
# TODO when restoring, could use a SizeQueue to make sure the db is kept busy

# TODO need to version the dumps, or something like that.
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

  # TODO need to also dump a first row containing useful stuff:
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
    return unless min && max
    # could possibly overrride Dataset#paginate(page_no, page_size, record_count=nil)
    # TODO definitely need to refactor this

    # will always include the last item because
    (min..max).step(options.page_size).each do |offset|
      page = table_dataset.where( id: offset...(offset+options.page_size) )
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
  # TODO lazy evaluation
  def restore( table_name, options = {} )
    logger.info "restoring #{table_name}"
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
