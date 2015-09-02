require 'rspec_syntax'
require 'pathname'

require Pathname(__dir__) + '../lib/wyrm/core_extensions.rb'

describe Method do
  describe '#kwargs_as_hash' do
    it 'empty for no keywords' do
      inst = Class.new do
        def without_kwargs( one, two )
          @kwargs = method(__method__).kwargs_as_hash(binding)
        end
      end.new
      inst.without_kwargs :one, :two
      inst.instance_variable_get('@kwargs').should == {}
    end

    it 'gives back hash of keywords' do
      inst = Class.new do
        def with_kwargs( one: 'one', two: 'two')
          @kwargs = method(__method__).kwargs_as_hash(binding)
        end
      end.new

      inst.with_kwargs
      inst.instance_variable_get('@kwargs').should == {one: 'one', two: 'two'}
    end

    it 'has correct values' do
      inst = Class.new do
        def with_kwargs( one: 'one', two: 'two')
          @kwargs = method(__method__).kwargs_as_hash(binding)
        end
      end.new

      inst.with_kwargs( one: 1, two: 2 )
      inst.instance_variable_get('@kwargs').should == {one: 1, two: 2}
    end

    it 'gets some default values' do
      inst = Class.new do
        def with_kwargs( one: 'one', two: 'two')
          @kwargs = method(__method__).kwargs_as_hash(binding)
        end
      end.new

      inst.with_kwargs( one: 1 )
      inst.instance_variable_get('@kwargs').should == {one: 1, two: 'two'}
    end
  end
end
