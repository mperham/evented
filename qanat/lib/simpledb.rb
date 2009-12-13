require "cgi"
require "base64"
require "openssl"
require "digest/sha1"
require 'xml'
require 'pp'

require 'fiber'
require 'em-http'

module Simpledb
  DEFAULT_HOST = 'sdb.amazonaws.com'
  API_VERSION = '2007-11-07'
  
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
    
    def amz_escape(param)
      param.to_s.gsub(/([^a-zA-Z0-9._~-]+)/n) do
        '%' + $1.unpack('H2' * $1.size).join('%').upcase
      end
    end

    def with_signature(verb, host=DEFAULT_HOST, path='/')
      hash = yield
      data = hash.keys.sort.map do |key|
        "#{amz_escape(key)}=#{amz_escape(hash[key])}"
      end.join('&')
      hash['Signature'] = URLencode(sign("#{verb}\n#{host}\n#{path}\n#{data}"))
      hash
    end

    def generate_request_hash(action, params={})
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
  
  class Database
    include Authentication
    
    def initialize(domain)
      @config = Qanat.load('sqs')
      @domain = domain
    end

    def get(id)
      request_hash = generate_request_hash("GetAttributes", 'ItemName' => id)
      http = async_operation(:get, request_hash, :timeout => timeout)
      code = http.response_header.status
      if code != 200
        logger.error "SDB got an error response: #{code} #{http.response}"
      end
      doc = parse_response(http.response)
      puts doc
    end
  
    def put(id, attribs)
      hash = { 'ItemName' => id }
      idx = 0
      attribs.each_pair do |k, v|
        hash["Attribute.#{idx}.Name"] = URLencode(k)
        hash["Attribute.#{idx}.Value"] = URLencode(v)
        idx = idx + 1
      end
      request_hash = generate_request_hash("PutAttributes", hash)
      http = async_operation(:post, { :body => request_hash, :timeout => timeout })
    end
    
    private
    
    def default_parameters
      #http://sdb.amazonaws.com/?AWSAccessKeyId=nosuchkey
        # &Action=GetAttributes
        # &DomainName=images-test
        # &ItemName=0000000000000000000000000000000000000001
        # &SignatureMethod=HmacSHA256
        # &SignatureVersion=2
        # &Timestamp=2009-12-12T20%3A30%3A03.000Z
        # &Version=2007-11-07
        # &Signature=P0wPnG7pbXjJ%2F0X8Uoclj4ZJXUl32%2Fog2ouegjGtIBU%3D
      { 
        "Timestamp" => Time.now.utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "Version" => API_VERSION,
        'DomainName' => @domain,
      }
    end

    def async_operation(method, parameters, opts)
      f = Fiber.current
      hash = with_signature(method.to_s.upcase) do
        parameters
      end
      p hash
      args = if method == :get
        { :query => hash }.merge(opts)
      else
        { :body => hash }.merge(opts)
      end
      http = EventMachine::HttpRequest.new("http://#{DEFAULT_HOST}/").send(method, args)
      http.callback { f.resume(http) }
      http.errback { f.resume(http) }

      return Fiber.yield
    end

    def logger
      DaemonKit.logger
    end

    def timeout
      Integer(@config['timeout'] || 10)
    end

    def parse_response(string)
      parser = XML::Parser.string(string)
      doc = parser.parse
      doc.root.namespaces.default_prefix = 'sdb'
      return doc
    end
  end
end