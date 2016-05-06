require 'rspec'

# include this here because at least once I forgot to update this
# when file names changed
require Pathname(__dir__) + '../lib/wyrm.rb'

include Wyrm

describe Wyrm do
  it 'has the right constants' do
    Wyrm.constants.sort.should == [:Dump, :Hole, :Logger, :Pump, :PumpMaker, :Restore, :SchemaTools, :VERSION, :STREAM_COMP, :STREAM_DCMP].sort
  end
end
