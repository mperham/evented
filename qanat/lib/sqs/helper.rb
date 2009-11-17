require "cgi"
require "base64"
require "rexml/document"
require "openssl"
require "digest/sha1"
require 'md5'
require 'xml'
require 'pp'

module SQS
  module Helper
    SIGNATURE_VERSION = "1"
    API_VERSION       = "2008-01-01"
    DEFAULT_HOST      = "queue.amazonaws.com"
    DEFAULT_PORT      = 80
    DEFAULT_PROTOCOL  = 'http'
    REQUEST_TTL       = 30
    DEFAULT_VISIBILITY_TIMEOUT = 30
    MAX_MESSAGE_SIZE  = (8 * 1024)

    @@digest = OpenSSL::Digest::Digest.new("sha1")

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

    def generate_request_hash(action, params={})
      message = params[:message]
      params.each { |key, value| params.delete(key) if (value.nil? || key.is_a?(Symbol)) }
      request_hash = { "Action"           => action,
                       "Expires"          => (Time.now + REQUEST_TTL).utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                       "AWSAccessKeyId"   => aws_access_key_id,
                       "Version"          => API_VERSION,
                       "SignatureVersion" => SIGNATURE_VERSION }
      request_hash["MessageBody"] = message if message
      request_hash.merge!(params)
      request_data = request_hash.sort{|a,b| (a[0].to_s.downcase)<=>(b[0].to_s.downcase)}.to_s
      request_hash['Signature'] = sign(request_data)
      logger.debug "request_hash:\n#{request_hash.pretty_inspect}"
      return request_hash
    end

    def parse_response(string)
      parser = XML::Parser.string(string)
      doc = parser.parse
      doc.root.namespaces.default_prefix = "sqs"
      return doc
    end
  end
end