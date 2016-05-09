require 'sequel'
require 'yaml'

require 'wyrm/logger'
require 'wyrm/module'

# TODO when restoring, could use a SizeQueue to make sure the db is kept busy
# TODO need to version the dumps, or something like that.
# TODO looks like io should belong to codec. Hmm. Not sure.
# TODO table_name table_dataset need some thinking about. Dataset would encapsulate both. But couldn't change db then, and primary_keys would be hard.
class Wyrm::Pump
  def initialize( db: nil, table_name: nil, io: STDOUT, codec: :marshal, page_size: 10000, dry_run: false, logger: nil )
    self.codec = codec
    self.db = db
    self.table_name = table_name
    self.io = io
    self.page_size = page_size
    self.dry_run = dry_run
    self.logger = logger
    yield self if block_given?
  end

  include Wyrm::Logger
  attr_writer :logger

  attr_accessor :io, :page_size, :dry_run
  def dry_run?; dry_run; end

  # These are affected by cached values
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
    return unless other_db

    # add extensions
    @db.extension :pagination

    # turn on postgres streaming if available
    # also gets called for non-postgres dbs, but that seems to be fine.
    if defined?( Sequel::Postgres::Database ) && @db.is_a?(Sequel::Postgres::Database) && defined?(Sequel::Postgres.supports_streaming?) && Sequel::Postgres.supports_streaming?
      @db.extension :pg_streaming
      logger.info "Streaming for #{@db.uri}"
    else
      logger.info "No streaming for #{@db.uri}"
    end
  end

  # return an object that responds to ===
  # which returns true if ==='s parameter
  # responds to all the methods
  def self.quacks_like( *methods )
    @quacks_like ||= {}
    @quacks_like[methods] ||= lambda do |inst|
      methods.all?{|m| inst.respond_to? m}
    end
  end

  def quacks_like( *methods )
    self.class.quacks_like( *methods )
  end

  def codec=( codec_thing )
    @codec =
    case codec_thing
    when :yaml; YamlCodec.new
    when :marshal; MarshalCodec.new
    when Class
      codec_thing.new
    when quacks_like(:encode,:decode)
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

  def primary_keys
    # each_with_object([]){...} is only faster for < 3 items in 100000
    @primary_keys ||= db.schema(table_name).map{|name,column_info| name if column_info[:primary_key]}.compact
  end

  def table_dataset
    @table_dataset ||= db[table_name.to_sym]
  end

  # Use limit / offset. Last fallback if there are no keys (or a compound primary key?).
  def paginated_dump( &encode_block )
    records_count = 0
    table_dataset.order(*primary_keys).each_page(page_size) do |page|
      logger.info "#{__method__} #{table_name} #{records_count}"
      logger.debug page.sql
      page.each &encode_block
      records_count += page_size
    end
  end

  # Use limit / offset, but not for all fields.
  # The idea is that large offsets are expensive in the db because the db server has to read
  # through the data set to reach the required offset. So make that only ids need to be read,
  # and then do the main select from the limited id list.
  # select * from massive as full
  #   inner join (select id from massive order by whatever limit m, n) limit
  #   on full.id = limit.id
  # order by full.whatever
  # http://www.numerati.com/2012/06/26/reading-large-result-sets-with-hibernate-and-mysql/
  def inner_dump( &encode_block )
    # could possibly overrride Dataset#paginate(page_no, page_size, record_count=nil)
    on_conditions = primary_keys.map{|f| [f,f]}.to_h
    (0..table_dataset.count).step(page_size).each do |offset|
      limit_dataset = table_dataset.select( *primary_keys ).limit( page_size, offset ).order( *primary_keys )
      page = table_dataset.join( limit_dataset, on_conditions ).order( *primary_keys ).qualify(table_name)
      logger.info "#{__method__} #{table_name} #{offset}"
      logger.debug page.sql
      page.each &encode_block
    end
  end

  # Selects pages by a range of ids, using >= and <.
  # Use this for integer pks
  def min_max_dump( &encode_block )
    # select max(id), min(id) from table
    # and then split that up into 10000 size chunks.
    # Not really important if there aren't exactly 10000
    min, max = table_dataset.select{[min(id), max(id)]}.first.values
    return unless min && max

    # will always include the last item because page_size will be
    # bigger than max for the last page
    (min..max).step(page_size).each do |offset|
      page = table_dataset.where( id: offset...(offset + page_size) )
      logger.info "#{__method__} #{table_name} #{offset}"
      logger.debug page.sql
      page.each &encode_block
    end
  end

  def stream_dump( &encode_block )
    logger.info "using result set streaming"

    # I want to output progress every page_size records,
    # without doing a records_count % page_size every iteration.
    # So define an external enumerator
    # TODO should really performance test the options here.
    records_count = 0
    enum = table_dataset.stream.enum_for
    loop do
      begin
        page_size.times do
          encode_block.call enum.next
          records_count += 1
        end
      ensure
        logger.info "#{__method__} #{table_name} #{records_count}" if records_count < page_size
        logger.debug "  #{records_count} from #{table_dataset.sql}"
      end
    end
  end

  # Dump the serialization of the table to the specified io.
  #
  # TODO need to also dump a first row containing useful stuff:
  # - source table name
  # - number of rows
  # - source db url
  # - permissions?
  # These should all be in one object that can be Marshall.load-ed easily.
  #
  # TODO could speed this up by have a query thread which runs the next page-query while
  # the current one is being written/compressed.
  def dump
    _dump do |row|
      codec.encode( row.values, io ) unless dry_run?
    end
  ensure
    io.flush
  end

  # decide which kind of paged iteration will be best for this table.
  # Return an iterator, or yield row hashes to the block
  def _dump( &encode_block )
    return enum_for(__method__) unless block_given?
    case
    when table_dataset.respond_to?( :stream )
      stream_dump &encode_block

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

  # start_row is zero-based
  #
  # TODO don't generate the full insert, ie leave out the fields
  # because we've already checked that the columns and the table
  # match.
  # TODO generate column names in insert, they might still work
  # if columns have been added to the db, but not the dump.
  def restore( start_row: 0, filename: 'io' )
    columns = table_dataset.columns
    row_enum = each_row

    return unless dump_matches_columns?( row_enum, columns )

    logger.info "#{__method__} inserting to #{table_name} from #{start_row}"
    logger.debug "  #{columns.inspect}"
    rows_restored = 0

    if start_row != 0
      logger.debug{ "skipping #{start_row} rows from #{filename}" }
      start_row.times do |i|
        row_enum.next
        logger.debug{ "skipped #{i} from #{filename}" } if i % page_size == 0
      end
      logger.debug{ "skipped #{start_row} from #{filename}" }
      rows_restored += start_row
    end

    loop do
      db.transaction do
        begin
          page_size.times do
            # This skips all the checks in the Sequel code. Basically we want
            # to generate the
            #   insert into (field1,field2) values (value1,value2)
            # statement as quickly as possible.
            #
            # Uses a private method so it will need to be updated repeatedly.
            sql = table_dataset.clone( columns: columns, values: row_enum.next ).send(:_insert_sql)
            db.execute sql unless dry_run?
            rows_restored += 1
          end
        rescue StopIteration
          # reached the end of the inout stream.
          # So commit this transaction, and then re-raise
          # StopIteration to get out of the loop{} statement
          db.after_commit{ raise StopIteration }
        end
      end
    end
    logger.info "#{__method__} #{table_name} done. Inserted #{rows_restored}."
    rows_restored
  end

  # Enumerate through the given io at its current position.
  # Can raise StopIteration (ie when eof is not detected)
  # MAYBE don't check for io.eof here, leave that to the codec
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
