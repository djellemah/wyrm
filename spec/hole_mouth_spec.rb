require 'rspec_syntax'
require 'pathname'

require Pathname(__dir__) + '../lib/wyrm/hole.rb'

describe Wyrm::Hole::Mouth do
if RUBY_VERSION == '2.1.0'
  it 'Queue broken on 2.1.0'
else
  describe '#flush' do
    it 'closes the queue' do
      subject.flush
      subject.queue.should be_empty
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
  end

  describe '#logger' do
    # this is here because the 2.1.0 without SizeQueue branch
    # has a logger which nothing else uses
    it 'works' do
      Wyrm::Hole::Mouth::RUBY_VERSION = '2.1.0'
      ->{subject.queue}.should raise_error(/broken in 2.1.0/)
      Wyrm::Hole::Mouth.send :remove_const, :RUBY_VERSION
    end
  end
end #unless RUBY_VERSION == '2.1.0'
end
