require 'rspec_syntax'
require 'pathname'

require Pathname(__dir__) + '../lib/wyrm/hole.rb'

describe Wyrm::Hole::Mouth do
  describe '#flush' do
    it 'closes the queue' do
      subject.flush
      subject.queue.should be_closed
    end
  end

  describe '#eof?' do
    describe 'flushed' do
      it 'true when queue empty and closed' do
        subject.flush
        subject.queue.should be_empty
        subject.queue.should be_closed
        subject.should be_eof
      end

      it 'false when queue open but empty' do
        subject.should_not be_eof
      end
    end

    describe 'not flushed' do
      it 'false when queue empty' do
        subject.queue.should be_empty
        subject.should_not be_eof
      end

      it 'false when queue has items' do
        rand(25).times{ subject.enq( :arg ) }
        subject.should_not be_eof
      end
    end
  end

  describe '#reset' do
    it 'clears queue' do
      rand(1..10).times{subject.enq :some_value}
      subject.queue.should_not be_empty
      subject.reset
      subject.queue.should be_empty
    end
  end

  describe '#deq' do
    it 'gets value' do
      subject.enq :montagne
      subject.deq.should == :montagne
    end

    it 'blocks for no values' do
      subject.queue.should be_empty
      th = Thread.new{subject.deq}
      sleep 0.05
      th.status.should == 'sleep'
      th.kill
      sleep 0.05
      th.status.should == false
    end

    it 'raises StopIteration for closed queue' do
      subject.queue.should be_empty
      subject.flush
      ->{subject.deq}.should raise_error(StopIteration)
    end
  end
end
