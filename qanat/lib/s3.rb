module S3
  USE_100_CONTINUE_PUT_SIZE = 1_000_000
  DEFAULT_HOST = 's3.amazonaws.com'
  AMAZON_HEADER_PREFIX   = 'x-amz-'
  AMAZON_METADATA_PREFIX = 'x-amz-meta-'

  class Bucket

    include Amazon::Authentication
    
    def initialize(bucket)
      @config = Qanat.load('sqs')
      @bucket = bucket
    end
    
    def put(key, data=nil, headers={})
      if (data.respond_to?(:lstat) && data.lstat.size >= USE_100_CONTINUE_PUT_SIZE) ||
         (data.respond_to?(:size)  && data.size       >= USE_100_CONTINUE_PUT_SIZE)
        headers['expect'] = '100-continue'
      end
      result = async_operation(:put, 
                               headers.merge(:key => CGI::escape(key), 
                                       "content-md5" => Base64.encode64(Digest::MD5.digest(data)).strip), 
                               data)
      code = result.response_header.status
      if code != 200
        raise ArgumentError, "S3 put failed: #{code} #{result.response}"
      end
    end

    def get(key, headers={}, &block)
      result = async_operation(:get, headers.merge(:key => CGI::escape(key)))
      code = result.response_header.status
      if code != 200
        raise ArgumentError, "S3 get failed: #{result.response}"
      end
      result.response
    end

    def head(key, headers={})
      result = async_operation(:head, headers.merge(:key => CGI::escape(key)))
      p result
    end

    def delete(key, headers={})
      result = async_operation(:delete, headers.merge(:key => CGI::escape(key)))
      p result
    end
    
    private

    def async_operation(method, headers={}, body=nil)
      f = Fiber.current
      path = generate_rest_request(method.to_s.upcase, headers)
      args = { :head => headers }
      args[:body] = body if body
      http = EventMachine::HttpRequest.new("http://#{DEFAULT_HOST}#{path}").send(method, args)
      http.callback { f.resume(http) }
      http.errback { f.resume(http) }

      return Fiber.yield
    end

    def canonical_string(method, path, headers={}, expires=nil) # :nodoc:
      s3_headers = {}
      headers.each do |key, value|
        key = key.downcase
        s3_headers[key] = value.to_s.strip if key[/^#{AMAZON_HEADER_PREFIX}|^content-md5$|^content-type$|^date$/o]
      end
      s3_headers['content-type'] ||= ''
      s3_headers['content-md5']  ||= ''
      s3_headers['date']           = ''      if s3_headers.has_key? 'x-amz-date'
      s3_headers['date']           = expires if expires
        # prepare output string
      out_string = "#{method}\n"
      s3_headers.sort { |a, b| a[0] <=> b[0] }.each do |key, value|
        out_string << (key[/^#{AMAZON_HEADER_PREFIX}/o] ? "#{key}:#{value}\n" : "#{value}\n")
      end
        # ignore everything after the question mark...
      out_string << path.gsub(/\?.*$/, '')
      out_string
    end

    def generate_rest_request(method, headers)  # :nodoc:
        # calculate request data
      path = "/#{@bucket}/#{headers[:key]}"
      headers.each{ |key, value| headers.delete(key) if (value.nil? || key.is_a?(Symbol)) }
      headers['content-type'] ||= ''
      headers['date']           = Time.now.httpdate
      auth_string = canonical_string(method, path, headers)
      signature   = sign(auth_string)
      headers['Authorization'] = "AWS #{aws_access_key_id}:#{signature}"
      path
    end
    
    def digester
      @@digest1
    end
    
    @@digest1 = OpenSSL::Digest::Digest.new("sha1")
  end
end
    