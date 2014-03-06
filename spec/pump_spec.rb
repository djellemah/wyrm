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
end
