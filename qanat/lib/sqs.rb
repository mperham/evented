require "cgi"
require "base64"
require "openssl"
require "digest/sha1"
require 'xml'
require 'pp'

require 'fiber'
require 'em-http'
require 'authentication'

def fiber_sleep(sec)
  f = Fiber.current
  EM.add_timer(sec) do
    f.resume
  end
  Fiber.yield
end

module SQS
  DEFAULT_HOST = "queue.amazonaws.com"
  API_VERSION = "2008-01-01"

  def self.run(&block)
    # Ensure graceful shutdown of the connection to the broker
    DaemonKit.trap('INT') { ::EM.stop }
    DaemonKit.trap('TERM') { ::EM.stop }

    # Start our event loop
    DaemonKit.logger.debug("EM.run")
    EM.run(&block)
  end

  class Queue
    REQUEST_TTL = 30
    
    include Amazon::Authentication

    def initialize(name)
      @config = Qanat.load('sqs')
      @name = name
    end
    
    def poll(concurrency, &block)
      concurrency.times do
        Fiber.new do
          while true
            receive_msg do |msg|
              block.call msg
            end
          end
        end.resume
      end
    end
    
    private
    
    def delete_msg(handle)
      logger.info "Deleting #{handle}"
      request_hash = generate_request_hash("DeleteMessage", 'ReceiptHandle' => handle)
      http = async_operation(:get, request_hash, :timeout => timeout)
      code = http.response_header.status
      if code != 200
        logger.error "SQS delete returned an error response: #{code} #{http.response}"
      end
    end
    
    def receive_msg(count=1, &block)
      request_hash = generate_request_hash("ReceiveMessage", 'MaxNumberOfMessages'  => count,
          'VisibilityTimeout' => 600)
      http = async_operation(:get, request_hash, :timeout => timeout)
      code = http.response_header.status
      if code == 200
        doc = Nokogiri::XML(http.response)
        msgs = doc.xpath('//xmlns:Message')
        if msgs.size > 0
          msgs.each do |msg|
            handle_el = msg.at_xpath('.//xmlns:ReceiptHandle')
            (logger.info msg; next) if !handle_el
          
            handle = msg.at_xpath('.//xmlns:ReceiptHandle').content
            message_id = msg.at_xpath('.//xmlns:MessageId').content
            checksum = msg.at_xpath('.//xmlns:MD5OfBody').content
            body = msg.at_xpath('.//xmlns:Body').content
                  
            if checksum != Digest::MD5.hexdigest(body)
              logger.info "SQS message does not match checksum, ignoring..."
            else
              block.call body
              delete_msg(handle)
            end
          end
        else
          logger.info "Queue #{@name} is empty"
        end
      else
        logger.error "SQS returned an error response: #{code} #{http.response}"
        # TODO parse the response and print something useful
        # TODO retry a few times with exponentially increasing delay
      end
    end
    
    def async_operation(method, parameters, opts)
      f = Fiber.current
      data = signed_parameters(parameters, method.to_s.upcase, DEFAULT_HOST, "/#{@name}")
      args = if method == :get
        { :query => data }.merge(opts)
      else
        { :body => data }.merge(opts)
      end
      http = EventMachine::HttpRequest.new("http://#{DEFAULT_HOST}/#{@name}").send(method, args)
      http.callback { f.resume(http) }
      http.errback { f.resume(http) }

      return Fiber.yield
    end

    def default_parameters
      request_hash = { "Expires" => (Time.now + REQUEST_TTL).utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                       "Version" => API_VERSION }
    end
    
    def logger
      DaemonKit.logger
    end

    def timeout
      Integer(@config['timeout'])
    end
  end
end