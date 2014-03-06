require 'rspec'

require Pathname(__dir__) + '../lib/wyrm/pump.rb'

include Wyrm

describe Pump do
  describe '.quacks_like' do
    it 'recognises method' do
      threequal = Pump.quacks_like( :tap )
      (threequal === Object.new).should be_true
    end

    it 'recognises two methods' do
      threequal = Pump.quacks_like( :tap, :instance_eval )
      (threequal === Object.new).should be_true
    end
  end

  describe '#table_name=' do
    it 'invalidates caches'
  end

  describe '#db=' do
    it 'invalidates caches'
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
