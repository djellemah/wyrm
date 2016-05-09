require 'rspec_syntax'

require Pathname(__dir__) + '../lib/wyrm/pump.rb'

include Wyrm

describe Pump do
  include DbConnections

  describe '.quacks_like' do
    it 'recognises method' do
      threequal = Pump.quacks_like( :tap )
      (threequal === Object.new).should == true
    end

    it 'recognises two methods' do
      threequal = Pump.quacks_like( :tap, :instance_eval )
      (threequal === Object.new).should == true
    end
  end

  describe '#table_name=' do
    it 'invalidates caches' do
      subject.should_receive(:invalidate_cached_members)
      subject.table_name = :big_face
    end
  end

  describe '#db=' do
    it 'invalidates caches' do
      subject.should_receive(:invalidate_cached_members)
      subject.db = sequel_sqlite_db
    end

    it 'handles nil db' do
      ->{subject.db = nil}.should_not raise_error
    end

    it 'adds pagination extension' do
      db = sequel_sqlite_db
      db.should_receive(:extension).with(:pagination)
      subject.db = db
    end

    it 'turns on streaming for postgres' do
      db = sequel_postgres_db
      pending "Sequel::Postgres::Database not defined" unless defined?(Sequel::Postgres::Database)
      db.should_receive(:extension).with(:pagination)
      db.should_receive(:extension).with(:pg_streaming)
      subject.db = db
    end

    it 'no streaming for non-postgres' do
      db = sequel_sqlite_db
      db.should_receive(:extension).with(:pagination)
      db.should_not_receive(:extension).with(:pg_streaming)
      subject.db = db
    end
  end

  describe '#codec=' do
    it ':yaml' do
      subject.codec = :yaml
      subject.codec.should be_a(Pump::YamlCodec)
    end

    it ':marshal' do
      subject.codec = :marshal
      subject.codec.should be_a(Pump::MarshalCodec)
    end

    def codec_class
      @codec_class ||=
      Class.new do
        def encode; end
        def decode; end
      end
    end

    it 'codec instance' do
      inst = codec_class.new
      subject.codec = inst
      subject.codec.should == inst
    end

    it 'codec class' do
      subject.codec = codec_class
      subject.codec.should be_a(codec_class)
    end

    it 'raise for unknown' do
      ->{subject.codec = Object.new}.should raise_error(/unknown codec/)
    end
  end
end
