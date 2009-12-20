require "cgi"
require "base64"
require "openssl"
require "digest/sha1"
require 'digest/md5'
require 'fiber'
require 'yaml'
require 'time'

require 'nokogiri'
require 'em-http'
require 'authentication'

require 'sqs'
require 'simpledb'
require 's3'

class Fiber
  def self.sleep(sec)
    f = Fiber.current
    EM.add_timer(sec) do
      f.resume
    end
    Fiber.yield
  end
end

module Qanat
  
  def self.run(&block)
    # Ensure graceful shutdown of the connection to the broker
    DaemonKit.trap('INT') { ::EM.stop }
    DaemonKit.trap('TERM') { ::EM.stop }

    # Start our event loop
    DaemonKit.logger.debug("EM.run")
    EM.run(&block)
  end

  def self.load(config)
    config = config.to_s
    config += '.yml' unless config =~ /\.yml$/

    hash = {}
    path = File.join( DAEMON_ROOT, 'config', config )
    hash.merge!(YAML.load_file( path )) if File.exists?(path)

    path = File.join( ENV['HOME'], ".qanat.#{config}" )
    hash.merge!(YAML.load_file( path )) if File.exists?(path)
    
    raise ArgumentError, "Can't find #{path}" if hash.size == 0

    hash[DAEMON_ENV]
  end
end
    