require 'sequel'
require 'yaml'
require 'ostruct'
require 'logger'
require 'fastandand'

Sequel.extension :migration

# TODO possibly use Gem::Package::TarWriter to write tar files
# TODO when restoring, could use a SizeQueue to make sure the db is kept busy

# TODO need to version the dumps, or something like that.
# TODO This really should be Wyrm::Hole. Or maybe Wyrm::Hole should
# be the codec that connects two DbPumps, for direct transfer?
class DbPump
  # some codecs might ignore io, eg if a dbpump is talking to another dbpump
  def initialize( db: nil, table_name: nil, io: STDOUT, codec: :marshal, page_size: 10000, dry_run: false )
    self.codec = codec
    self.db = db
    self.table_name = table_name
    self.io = io
    self.page_size = page_size
    self.dry_run = dry_run
    yield self if block_given?
  end

  attr_accessor :io, :page_size, :dry_run
  def dry_run?; dry_run; end

  # These affect cached values
  attr_reader :db, :table_name

  def invalidate_cached_members
    @primary_keys = nil
    @table_dataset = nil
  end

  def table_name=( name_sym )
    invalidate_cached_members
    @table_name = name_sym
  end

  def db=( other_db )
    invalidate_cached_members
    @db = other_db
    @db.extension :pagination
  end

  # return an object that responds to ===
  # which returns true if ==='s parameter
  # responds to all the methods
  def quacks_like( *methods )
    @quacks_like ||= {}
    @quacks_like[methods] ||= Object.new.tap do |obj|
      obj.define_singleton_method(:===) do |instance|
        methods.all?{|m| instance.respond_to? m}
      end
    end
  end

  def codec=( codec_thing )
    @codec =
    case codec_thing
    when :yaml; YamlCodec.new
    when :marshal; MarshalCodec.new
    when Class
      codec_thing.new
    when quacks_like( :encode, :decode )
      codec_thing
    else
      raise "unknown codec #{codec_thing.inspect}"
    end
  end

  attr_reader :codec

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
  def paginated_dump( &encode_block )
    table_dataset.order(*primary_keys).each_page(page_size) do |page|
      logger.info page.sql
      page.each &encode_block
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
  def inner_dump( &encode_block )
    # could possibly overrride Dataset#paginate(page_no, page_size, record_count=nil)
    0.step(table_dataset.count, page_size).each do |offset|
      limit_dataset = table_dataset.select( *primary_keys ).limit( page_size, offset ).order( *primary_keys )
      page = table_dataset.join( limit_dataset, Hash[ primary_keys.map{|f| [f,f]} ] ).order( *primary_keys ).qualify(table_name)
      logger.info page.sql
      page.each &encode_block
    end
  end

  # Selects pages by a range of ids, using >= and <.
  # Use this for integer pks
  def min_max_dump( &encode_block )
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
      page.each &encode_block
    end
  end

  # Dump the serialization of the table to the specified io.
  # TODO need to also dump a first row containing useful stuff:
  # - source table name
  # - number of rows
  # - source db url
  # - permissions?
  # These should all be in one object that can be Marshall.load-ed easily.
  def dump
    _dump do |row|
      codec.encode( row.values, io ) unless dry_run?
    end
    io.flush
  end

  # decide which kind of paged iteration will be best for this table.
  # Return an iterator, or yield row hashes to the block
  def _dump( &encode_block )
    return enum_for(__method__) unless block_given?
    case
    when primary_keys.empty?
      paginated_dump &encode_block
    when primary_keys.all?{|i| i == :id }
      min_max_dump &encode_block
    else
      inner_dump &encode_block
    end
  end

  def dump_matches_columns?( row_enum, columns )
    raise "schema mismatch" unless row_enum.peek.size == columns.size
    true
  rescue StopIteration
    # peek threw a StopIteration, so there's no data
    false
  end

  # TODO lazy evaluation / streaming
  # TODO don't generate the full insert, ie leave out the fields
  # because we've already checked that the columns and the table
  # match.
  # TODO generate column names in insert, they might still work
  # if columns have been added to the db, but not the dump.
  # start_row is zero-based
  def restore( start_row: 0, filename: 'io' )
    columns = table_dataset.columns
    row_enum = each_row

    return unless dump_matches_columns?( row_enum, columns )

    logger.info{ "inserting to #{table_name} #{columns.inspect}" }
    rows_restored = 0

    if start_row != 0
      logger.info{ "skipping #{start_row} rows from #{filename}" }
      start_row.times do |i|
        row_enum.next
        logger.info{ "skipped #{i} from #{filename}" } if i % page_size == 0
      end
      logger.info{ "skipped #{start_row} from #{filename}" }
      rows_restored += start_row
    end

    logger.info{ "inserting to #{table_name} from #{rows_restored}" }

    loop do
      db.transaction do
        begin
          page_size.times do
            # This skips all the checks in the Sequel code
            sql = table_dataset.clone( columns: columns, values: row_enum.next ).send( :clause_sql, :insert )
            db.execute sql unless dry_run?
            rows_restored += 1
          end
        rescue StopIteration
          # er reached the end of the inout stream.
          # So commit this transaction, and then re-raise
          # StopIteration to get out of the loop{} statement
          db.after_commit{ raise StopIteration }
        end
        logger.info{ "#{table_name} inserted #{rows_restored}" }
      end
    end
    logger.info{ "#{table_name} done. Inserted #{rows_restored}." }
    rows_restored
  end

  # Enumerate through the given io at its current position
  # TODO don't check for io.eof here, leave that to the codec
  def each_row
    return enum_for(__method__) unless block_given?
    yield codec.decode( io ) until io.eof?
  end

  # Enumerate sql insert statements from the dump
  def insert_sql_each
    return enum_for(__method__) unless block_given?
    each_row do |row|
      yield table_dataset.insert_sql( row )
    end
  end
end
