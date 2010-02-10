require 'yaml'
require 'erb'
require 'active_record'

RAILS_ENV=DaemonKit.configuration.environment

ActiveRecord::Base.configurations = YAML::load(ERB.new(File.read(File.join(DAEMON_ROOT, 'config', 'database.yml'))).result)
ActiveRecord::Base.default_timezone = :utc
ActiveRecord::Base.logger = DaemonKit.logger
ActiveRecord::Base.logger.level = Logger::INFO
ActiveRecord::Base.time_zone_aware_attributes = true
Time.zone = 'UTC'
ActiveRecord::Base.establish_connection

# Your custom message dispatch logic goes below.  This is
# a sample of how to do it but you can modify as necessary.
#
# Our message processing classes go in lib/processors.
#
# Example message:
#   { :msg_type => 'index_page', :page_id => 15412323 }
#
# Qanat will execute:
#   processor = IndexPage.new
#   processor.process(hash)
# to handle the message.

$:.unshift(File.expand_path(File.join(File.dirname(__FILE__), 'processors')))
# require 'your_message_processor1'
# require 'your_message_processor2'

module Qanat

  def self.dispatch(msg)
    hash = YAML::load(msg)
    name = hash.fetch(:msg_type).to_s.camelize
    profile(hash) do
      name.constantize.new.process(hash)
    end
  end
  
  def self.profile(hash)
    a = Time.now
    return yield
  ensure
    DaemonKit.logger.info("Processed message: #{hash.inspect} in #{Time.now - a} sec")
  end
  
end

    