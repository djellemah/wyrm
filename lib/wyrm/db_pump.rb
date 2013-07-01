require 'sequel'
require 'yaml'
require 'ostruct'
require 'logger'
require 'fastandand'

Sequel.extension :migration, :schema_dumper, :pagination

# TODO possibly use Gem::Package::TarWriter to write tar files
# TODO when restoring, could use a SizeQueue to make sure the db is kept busy

# TODO need to version the dumps, or something like that.
# So the slowest-changing variables are the db, the io stream
# and the page size.
# table will change every call. Will IO stream change between
# table changes? No. So a currying type approach will work.
# Somebody must have done this before.
# But table and io are often related (ie table going to one file)
# TODO This really should be Wyrm::Hole. Or maybe Wyrm::Hole should
# be the codec that connects two DbPumps, for direct transfer?
class DbPump
  # some codecs might ignore io, eg if a dbpump is talking to another dbpump
  def initialize( db, table_name, io: STDOUT, codec: :marshal, page_size: 10000, dry_run: false )
    self.codec = codec
    self.db = db
    self.table_name = table_name
    self.io = io
    self.page_size = page_size
    self.dry_run = dry_run
    yield self if block_given?
  end

  attr_accessor :io, :page_size, :dry_run

  # These affect cached values
  attr_reader :db, :table_name

  def table_name=( name_sym )
    @primary_keys = nil
    @table_dataset = nil
    @table_name = name_sym
  end

  def db=( other_db )
    @primary_keys = nil
    @table_dataset = nil
    @db = other_db
  end

  def dry_run?; dry_run; end

  class RespondsTo
    def initialize( *methods )
      @methods = methods
    end

    def ===( instance )
      @methods.all?{|m| instance.respond_to? m}
    end
  end

  def codec=( codec_thing )
    @codec =
    case codec_thing
    when :yaml; YamlCodec.new
    when :marshal; MarshalCodec.new
    when Class
      codec.new
    when RespondsTo.new( :encode, :decode )
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

  def primary_keys
    @primary_keys ||= db.schema(table_name).select{|df| df.last[:primary_key]}.map{|df| df.first}
  end

  def table_dataset
    @table_dataset ||= db[table_name.to_sym]
  end

  # TODO possibly use select from outer / inner join to
  # http://www.numerati.com/2012/06/26/reading-large-result-sets-with-hibernate-and-mysql/
  # because mysql is useless
  def paginated_dump
    table_dataset.order(*primary_keys).each_page(page_size) do |page|
      logger.info page.sql
      page.each do |row|
        unless dry_run?
          codec.encode row.values, io
        end
      end
    end
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
  def inner_dump
    # could possibly overrride Dataset#paginate(page_no, page_size, record_count=nil)
    0.step(table_dataset.count, page_size).each do |offset|
      limit_dataset = table_dataset.select( *primary_keys ).limit( page_size, offset ).order( *primary_keys )
      page = table_dataset.join( limit_dataset, Hash[ primary_keys.map{|f| [f,f]} ] ).order( *primary_keys ).qualify_to(table_name)
      logger.info page.sql
      page.each do |row|
        unless dry_run?
          codec.encode row.values, io
        end
      end
    end
  end

  # TODO need to also dump a first row containing useful stuff:
  # - source table name
  # - number of rows
  # - source db url
  # - permissions?
  # These should all be in one object that can be Marshall.load-ed easily.
  def dump
    case
    when primary_keys.empty?
      paginated_dump
    when primary_keys.all?{|i| i == :id }
      min_max_dump
    else
      inner_dump
    end
    io.flush
  end

  # could use this for integer pks
  def min_max_dump
    # select max(id), min(id) from patents
    # and then split that up into 10000 size chunks. Not really important if there aren't exactly 10000
    min, max = table_dataset.select{[min(id), max(id)]}.first.values
    return unless min && max
    # could possibly overrride Dataset#paginate(page_no, page_size, record_count=nil)
    # TODO definitely need to refactor this

    # will always include the last item because
    (min..max).step(page_size).each do |offset|
      page = table_dataset.where( id: offset...(offset + page_size) )
      logger.info page.sql
      page.each do |row|
        unless dry_run?
          codec.encode row.values, io
        end
      end
    end
  end

  # TODO possible memory issues here if the rows are big. May need to fork this.
  # TODO lazy evaluation
  def restore( start_row: 0 )
    logger.info "restoring #{table_name}"
    # destination db should be same structure as incoming data
    column_names = db.schema(table_name.to_sym).map( &:first )
    first = ->(row){raise "schema mismatch" if row.size != column_names.size}

    rows_restored = 0

    # skip this many rows
    start_row.times do
      codec.decode( io ) {|row|}
    end

    # copy rows into db
    while !io.eof?
      # fetch a page of rows
      rows_ary = []
      begin
        page_size.times do |i|
          codec.decode( io ) do |row|
            rows_ary << row
          end
          rows_restored += 1
        end
      rescue EOFError => e
        # ran out of rows, so just use the ones we have so far
      end

      # insert to db. Hopeful db support bulk insert, which Sequel will figure out
      db.transaction do
        table_dataset.import column_names, rows_ary
        yield rows_restored if block_given?
        logger.info "restored #{rows_restored}"
      end
    end

    rows_restored
  end

  def self.from_bz2( filename, db, table_name, options = {} )
    IO.popen( "pbzip2 -d -c #{filename}" ) do |io|
      dbpump = DbPump.new db, table_name, io: io
      dbpump.restore
    end
  end
end
