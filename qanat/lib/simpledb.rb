module Simpledb
  class Database
    include AmazonHelper
    
    DEFAULT_HOST = 'sdb.amazonaws.com'
    API_VERSION = '2007-11-07'
    
    def initialize(domain)
      @domain = domain
    end

    def get(id)
      request_hash = generate_request_hash("GetAttributes", 'ItemName' => id)
      http = async_operation(:get, { :query => request_hash, :timeout => timeout })
      code = http.response_header.status
      if code != 200
        logger.error "SQS delete returned an error response: #{code} #{http.response}"
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
        idx++
      end
      request_hash = generate_request_hash("PutAttributes", hash)
      http = async_operation(:post, { :body => request_hash, :timeout => timeout })
    end
    
    private
    
    def default_parameters
      #GET http:///?AWSAccessKeyId=14MHSD78AFZA0999PMR2&Action=CreateDomain&DomainName=images-test&SignatureMethod=HmacSHA256&SignatureVersion=2&Timestamp=2009-12-12T20%3A27%3A42.000Z&Version=2007-11-07&Signature=uoGQYZTJRQ%2BbM2MBjRvEz2UCcAbezKjAgbbfuoC0T00%3D
      #http://sdb.amazonaws.com/?AWSAccessKeyId=14MHSD78AFZA0999PMR2
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

    def async_operation(method, args)
      f = Fiber.current
      http = EventMachine::HttpRequest.new("http://#{DEFAULT_HOST}/#{@name}").send(method, args)
      http.callback { f.resume(http) }
      http.errback { f.resume(http) }

      return Fiber.yield
    end

    def timeout
      Integer(@config['timeout'] || 10)
    end

    def default_prefix
      'sdb'
    end

  end
end