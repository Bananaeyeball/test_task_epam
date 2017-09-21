require 'bundler/setup'
require 'active_support/all'

module Rails
  module_function

  def env
    'test'
  end

  def root
    Dir.pwd
  end

  def logger
    @logger ||= Class.new do
      def info(*args)
      end
    end.new
  end
end

RSpec.configure do |config|
  config.mock_with :rspec do |c|
    c.syntax = :should
  end

  config.expect_with :rspec do |c|
    c.syntax = %i[should expect]
  end
end

