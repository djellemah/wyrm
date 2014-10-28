require 'rspec_syntax'

require Pathname(__dir__) + '../lib/wyrm/hole.rb'

describe Wyrm::Hole::Mouth do
if RUBY_VERSION == '2.1.0'
  it 'Queue broken on 2.1.0'
else
  describe '#flush' do
    it 'calls poison_queue' do
      subject.should_receive(:poison_queue)
      subject.flush
    end

    it 'sets flag' do
      subject.instance_variable_get('@flushed').should_not == true
      subject.flush
      subject.instance_variable_get('@flushed').should == true
    end

    describe 'queue empty with waiters' do
      THREADS = rand(1..7)
      def waiters
        @waiters ||= THREADS.times.map do
          Thread.new do
            values = []
            begin
              until subject.eof?
                values << subject.deq
                sleep( rand * 0.05 )
              end
              [:eoffed, values]
            rescue StopIteration
              [:poisoned, values]
            end
          end
        end
      end

      it 'poisons queue' do
        waiters
        # wait for thread setup to finish
        sleep 0.1
        subject.flush
        thread_values = waiters.map {|waiter| waiter.join(4).andand.value }
        thread_values.map(&:first).should == THREADS.times.map{:poisoned}
      end

      it 'eof queue' do
        50.times{subject.enq 'hello'}
        waiters
        subject.flush
        thread_values = waiters.map {|waiter| waiter.join(4).andand.value }
        thread_values.map(&:first).should == THREADS.times.map{:eoffed}
      end
    end
  end

  describe '#eof?' do
    describe 'flushed' do
      before :each do
        subject.flush
      end

      it 'true when queue empty' do
        subject.queue.should be_empty
        subject.should be_eof
      end

      it 'false when queue empty' do
        subject.enq( :arg )
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
    it 'resets flushed' do
      subject.instance_variable_set '@flushed', true
      subject.reset
      subject.instance_variable_get('@flushed').should == false
    end

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

    it 'raise StopIteration on poison' do
      subject.queue.should be_empty
      waiter = Thread.new { subject.deq }
      sleep(0.01) while subject.queue.num_waiting != 1
      subject.poison_queue

      # this is so cool. Thread#value will re-raise the exception it caught
      ->{waiter.value}.should raise_error(StopIteration)

      # queue should be empty now
      subject.queue.should be_empty
    end

    it 're-queues poison' do
      subject.queue << :poison
      subject.should_receive(:poison_queue)
      ->{subject.deq}.should raise_error(StopIteration)
    end
  end

  describe '#poison_queue' do
    it 'poisons when queue empty with waiters' do
      subject.queue.should be_empty

      # there has to be a thread waiting for the poison to be added
      waiter = Thread.new { subject.queue.deq }
      sleep(0.01) while subject.queue.num_waiting != 1
      subject.poison_queue

      waiter.value.should == :poison

      # queue should be empty now
      subject.queue.should be_empty
    end

    it 'no poison when queue empty' do
      subject.queue.should be_empty
      subject.poison_queue
      subject.queue.should be_empty
    end

    it 'no poison for no waiters' do
      subject.queue << :hello
      subject.queue << :there
      subject.poison_queue
      subject.queue.size.should == 2
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
