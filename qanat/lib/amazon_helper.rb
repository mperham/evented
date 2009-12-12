require "cgi"
require "base64"
require "openssl"
require "digest/sha1"
require 'xml'
require 'pp'
require 'active_support'

module AmazonHelper

  SIGNATURE_VERSION = "1"
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
    returning(yield) do |hash|
      data = hash.sort{|a,b| (a[0].to_s.downcase)<=>(b[0].to_s.downcase)}.join('')
      hash['Signature'] = URLencode(sign(data))
    end
  end
  
  def generate_request_hash(action, params={})
    with_signature do
      request_hash = { 
        "Action" => action,
        "SignatureMethod" => 'HmacSHA256'
        "AWSAccessKeyId" => aws_access_key_id,
        "SignatureVersion" => SIGNATURE_VERSION,
      }
#      request_hash["MessageBody"] = message if message
      request_hash.merge(default_parameters).merge(params)
    end
  end

  def parse_response(string)
    parser = XML::Parser.string(string)
    doc = parser.parse
    doc.root.namespaces.default_prefix = default_prefix
    return doc
  end
end