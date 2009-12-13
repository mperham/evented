require "cgi"
require "base64"
require "openssl"
require "digest/sha1"
require 'xml'
require 'pp'

require 'fiber'
require 'em-http'

def fiber_sleep(sec)
  f = Fiber.current
  EM.add_timer(sec) do
    f.resume
  end
  Fiber.yield
end

module SQS
  def self.run(&block)
    # Ensure graceful shutdown of the connection to the broker
    DaemonKit.trap('INT') { ::EM.stop }
    DaemonKit.trap('TERM') { ::EM.stop }

    # Start our event loop
    DaemonKit.logger.debug("EM.run")
    EM.run(&block)
  end
  
  module Authentication
    SIGNATURE_VERSION = "2"
    @@digest = OpenSSL::Digest::Digest.new("sha256")

    def sign(auth_string)
      Base64.encode64(OpenSSL::HMAC.digest(@@digest, aws_secret_access_key, auth_string)).strip
    end

    # From Amazon's SQS Dev Guide, a brief description of how to escape:
    # "URL encode the computed signature and other query parameters as specified in 
    # RFC1738, section 2.2. In addition, because the + character is interpreted as a blank space 
    # by Sun Java classes that perform URL decoding, make sure to encode the + character 
    # although it is not required by RFC1738."
    # Avoid using CGI::escape to escape URIs. 
    # CGI::escape will escape characters in the protocol, host, and port
    # sections of the URI.  Only target chars in the query
    # string should be escaped.
    def URLencode(raw)
      e = URI.escape(raw)
      e.gsub(/\+/, "%2b")
    end

    def aws_access_key_id
      @config['access_key']
    end

    def aws_secret_access_key
      @config['secret_key']
    end

    def with_signature
      hash = yield
      data = hash.sort{|a,b| (a[0].to_s.downcase)<=>(b[0].to_s.downcase)}.join('')
      hash['Signature'] = URLencode(sign(data))
      hash
    end

    def generate_request_hash(action, params={})
      with_signature do
        request_hash = { 
          "Action" => action,
          "SignatureMethod" => 'HmacSHA256',
          "AWSAccessKeyId" => aws_access_key_id,
          "SignatureVersion" => SIGNATURE_VERSION,
        }
  #      request_hash["MessageBody"] = message if message
        request_hash.merge(default_parameters).merge(params)
      end
    end
  end  

  class Queue
    DEFAULT_HOST      = "queue.amazonaws.com"
    REQUEST_TTL       = 30
    API_VERSION       = "2008-01-01"
    
    include Authentication

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
      http = async_operation(:get, :query => request_hash, :timeout => timeout)
      code = http.response_header.status
      if code != 200
        logger.error "SQS delete returned an error response: #{code} #{http.response}"
      end
    end
    
    def receive_msg(count=1, &block)
      request_hash = generate_request_hash("ReceiveMessage", 'MaxNumberOfMessages'  => count,
          'VisibilityTimeout' => 600)
      http = async_operation(:post, { :body => request_hash, :timeout => timeout })
      code = http.response_header.status
      doc = parse_response(http.response)
      msgs = doc.find('//sqs:Message')
      if msgs.size > 0
        msgs.each do |msg|
          handle_el = msg.find_first('//sqs:ReceiptHandle')
          (logger.info msg; next) if !handle_el
          
          handle = msg.find_first('//sqs:ReceiptHandle').content.strip
          message_id = msg.find_first('//sqs:MessageId').content.strip
          checksum = msg.find_first('//sqs:MD5OfBody').content.strip
          body = msg.find_first('//sqs:Body').content.strip
                  
          if checksum != Digest::MD5.hexdigest(body)
            logger.info "SQS message does not match checksum, ignoring..."
          else
            logger.info "Queued message, SQS message id is: #{message_id}"
            block.call body
            delete_msg(handle)
          end
        end
      elsif code == 200
        logger.info "Queue #{@name} is empty"
        fiber_sleep(5)
      else
        logger.error "SQS returned an error response: #{code} #{http.response}"
        # TODO parse the response and print something useful
        # TODO retry a few times with exponentially increasing delay
      end
    end
    
    def async_operation(method, args)
      f = Fiber.current
      http = EventMachine::HttpRequest.new("http://#{DEFAULT_HOST}/#{@name}").send(method, args)
      http.callback { f.resume(http) }
      http.errback { f.resume(http) }

      return Fiber.yield
    end

    def default_parameters
      request_hash = { "Expires"          => (Time.now + REQUEST_TTL).utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                       "Version"          => API_VERSION }
    end
    
    def default_prefix
      'sqs'
    end
    
    def logger
      DaemonKit.logger
    end

    def timeout
      Integer(@config['timeout'])
    end

    def parse_response(string)
      parser = XML::Parser.string(string)
      doc = parser.parse
      doc.root.namespaces.default_prefix = default_prefix
      return doc
    end
  end
end