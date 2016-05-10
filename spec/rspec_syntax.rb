require 'rspec'

# --enable-frozen-string-literal-debug
# RubyVM::InstructionSequence.compile_option = {frozen_string_literal: true}

# turn off the "old syntax" warnings
RSpec.configure do |config|
  config.mock_with :rspec do |c|
    c.syntax = [:should, :expect]
  end

  config.expect_with :rspec do |c|
    c.syntax = [:should, :expect]
  end
end
