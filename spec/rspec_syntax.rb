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

module DbConnections
  def sequel_sqlite_db
    if RUBY_ENGINE == 'jruby'
      # NOTE trailing : is meaningful to sqlite
      Sequel.connect 'jdbc:sqlite::memory:'
    else
      Sequel.sqlite
    end
  end

  def sequel_postgres_db
    if RUBY_ENGINE == 'jruby'
      Sequel.connect "jdbc:postgresql://localhost/#{ENV['USER']}?user=#{ENV['USER']}"
    else
      Sequel.postgres
    end
  end
end
