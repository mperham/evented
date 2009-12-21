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
require 'sdb'
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