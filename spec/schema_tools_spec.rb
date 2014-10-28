require 'rspec_syntax'
require 'sequel'
require 'sqlite3'

require Pathname(__dir__) + '../lib/wyrm/schema_tools.rb'
require Pathname(__dir__) + '../lib/wyrm/logger.rb'

include Wyrm

describe SchemaTools do
  after :each do
    @src_dst = nil
  end

  class Includer
    include SchemaTools
    include Wyrm::Logger
    def logger
      lgr = super
      # silence most logging
      lgr.level = ::Logger::FATAL
      lgr
    end
    def initialize( src_db, dst_db )
      @src_db = src_db.andand.extension :schema_dumper
      @dst_db = dst_db
    end
    attr_reader :src_db, :dst_db
  end

  def src_dst( src_db: Sequel.sqlite, dst_db: Sequel.sqlite )
    @src_dst ||= Includer.new src_db, dst_db
  end

  def with_src
    src_dst dst_db: nil
  end

  def with_dst
    src_dst src_db: nil
  end

  describe '#same_db' do
    it 'for same db' do
      src_dst.same_db.should == true
    end

    it 'for different db' do
      src_dst( dst_db: Sequel.postgres ).same_db.should == false
    end
  end

  describe 'src_db dependencies' do
    describe '#schema_migration' do
      it 'executes' do
        String.should === with_src.schema_migration
        with_src.schema_migration.should =~ /Sequel.migration/
        with_src.schema_migration.should =~ /change/
      end

      it 'recreates table' do
        with_src.src_db.create_table(:things){|t| primary_key :id; t.String :name}
        with_src.schema_migration.should =~ /create_table/
      end
    end

    describe '#index_migration' do
      it 'executes' do
        String.should === with_src.index_migration
        with_src.index_migration.should =~ /Sequel.migration/
        with_src.index_migration.should =~ /change/
      end

      it 'recreates table' do
        with_src.src_db.create_table(:things){|t| primary_key :id; t.String :name}
        with_src.src_db.add_index(:things, :name)
        with_src.index_migration.should =~ /add_index/
      end
    end

    describe '#fk_migration' do
      it 'executes' do
        String.should === with_src.fk_migration
        with_src.fk_migration.should =~ /Sequel.migration/
        with_src.fk_migration.should =~ /change/
      end

      it 'recreates table' do
        with_src.src_db.create_table(:things){|t| primary_key :id; t.String :name}
        with_src.src_db.create_table(:times){|t| primary_key :id; t.foreign_key :thing_id, :things}
        with_src.fk_migration.should =~ /add_foreign_key/
      end
    end
  end

  describe '#drop_table_options' do
    it 'empty for non-postgres' do
      with_dst.drop_table_options.should == {}
    end

    it 'cascade for postgres' do
      src_dst(dst_db: Sequel.postgres, src_db: nil).drop_table_options.should == {cascade: true}
    end
  end

  describe '#drop_tables' do
    #( tables )
    it 'removes tables with no foreign keys' do
      with_dst.dst_db.create_table(:things){|t| primary_key :id; t.String :name}
      with_dst.dst_db.create_table(:times){|t| primary_key :id}

      with_dst.dst_db.tables.should == %i[things times]
      with_dst.drop_tables with_dst.dst_db.tables
      with_dst.dst_db.tables.should be_empty
    end

    it 'removes tables with some foreign keys' do
      with_dst.dst_db.create_table(:things){|t| primary_key :id; t.String :name}
      with_dst.dst_db.create_table(:times){|t| primary_key :id; t.foreign_key :thing_id, :things}

      with_dst.dst_db.tables.should == %i[things times]
      with_dst.drop_tables with_dst.dst_db.tables
      with_dst.dst_db.tables.should be_empty
    end

    if ::SQLite3::SQLITE_VERSION >= "3.6.19"
      it 'sqlite mutual foreign keys' do
        with_dst.dst_db.create_table(:things){|t| primary_key :id; t.String :name}
        with_dst.dst_db.create_table(:times){|t| primary_key :id; t.foreign_key :thing_id, :things}
        with_dst.dst_db.alter_table :things do
          add_foreign_key :times_id, :times
        end

        thing_id = with_dst.dst_db[:things].insert name: 'Gruffalo'
        time_id = with_dst.dst_db[:times].insert thing_id: thing_id
        with_dst.dst_db[:things].update times_id: time_id

        with_dst.dst_db.tables.sort.should == %i[things times]
        ->{with_dst.drop_tables with_dst.dst_db.tables}.should raise_error(/mutual foreign keys/)
      end
    else
      it "Can't test foreign keys with sqlite < 3.6.19"
    end

    it 'handles mysql foreign key exception'
  end

  describe '#create_tables' do
    MIGRATION = <<-EOF
      Sequel.migration do
        change do
          create_table(:things) do
            primary_key :id
            String :name, :size=>255
            Integer :times_id
          end

          create_table(:times) do
            primary_key :id
            foreign_key :thing_id, :things
          end

          alter_table(:things) do
            add_foreign_key [:times_id], :times, :key=>nil
          end
        end
      end
    EOF

    it 'creates table from schema_migration' do
      with_dst.stub( :schema_migration ) do
        MIGRATION
      end
      with_dst.dst_db.tables.should be_empty
      with_dst.create_tables
      with_dst.dst_db.tables.sort.should == %i[things times]
    end
  end

  # just receive the methods
  describe '#create_indexes' do
    it 'creates indexes and foreign keys' do
      with_dst.should_receive(:index_migration){ 'Sequel.migration{}' }
      with_dst.should_receive(:fk_migration){ 'Sequel.migration{}' }
      with_dst.create_indexes
    end

    it 'resets key sequences for postgres' do
      with_dst.stub(:index_migration){ 'Sequel.migration{}' }
      with_dst.stub(:fk_migration){ 'Sequel.migration{}' }

      with_dst.dst_db.stub(:database_type){Sequel.postgres.database_type}

      with_obj = Object.new
      with_dst.dst_db.should_receive(:tables){[with_obj]}
      with_dst.dst_db.should_receive(:reset_primary_key_sequence).with(with_obj)
      with_dst.create_indexes
    end
  end

end
