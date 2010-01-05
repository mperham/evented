module SDB
  DEFAULT_HOST = 'sdb.amazonaws.com'
  API_VERSION = '2009-04-15'
  
  class Database
    include Amazon::Authentication
    
    def initialize(domain)
      @config = Qanat.load('amzn')
      @domain = domain
    end

=begin
<?xml version="1.0" encoding="UTF-8"?>
<GetAttributesResponse xmlns="http://sdb.amazonaws.com/doc/2007-11-07/">
  <GetAttributesResult>
    <Attribute>
      <Name>alt</Name>
      <Value>alife-40-below-4.jpg</Value>
    </Attribute>
    <Attribute>
      <Name>fetch_url</Name>
      <Value>http://thekaoseffect.com/blog/wp-content/uploads/2009/11/alife-40-below-4.jpg</Value>
    </Attribute>
    <Attribute>
      <Name>created_at</Name>
      <Value>20091105173159</Value>
    </Attribute>
  </GetAttributesResult>
  <ResponseMetadata>
    <RequestId>4842bf3b-cbdd-3f35-406d-1becc842b18c</RequestId>
    <BoxUsage>0.0000093382</BoxUsage>
  </ResponseMetadata>
</GetAttributesResponse>
=end
    def get(id_or_array)
      request_hash = generate_request_hash("GetAttributes", 'ItemName' => id_or_array)
      http = async_operation(:get, request_hash, :timeout => timeout)
      code = http.response_header.status
      if code != 200
        logger.error "SDB got an error response: #{code} #{http.response}"
        return nil
      end
      to_attributes(http.response)
    end
  
    def put(id, attribs)
      hash = { 'ItemName' => id }
      idx = 0
      attribs.each_pair do |k, v|
        hash["Attribute.#{idx}.Name"] = CGI::escape(k.to_s)
        hash["Attribute.#{idx}.Value"] = CGI::escape(v.to_s)
        idx = idx + 1
      end
      request_hash = generate_request_hash("PutAttributes", hash)
      http = async_operation(:post, request_hash, :timeout => timeout)
    end
    
    private
    
    def to_attributes(doc)
      attributes = {}
      xml = Nokogiri::XML(doc)
      xml.xpath('//xmlns:Attribute').each do |node|
        k = node.at_xpath('.//xmlns:Name').content
        v = node.at_xpath('.//xmlns:Value').content
        if attributes.has_key?(k)
          if !attributes[k].is_a?(Array)
            attributes[k] = Array(attributes[k])
          end
          attributes[k] << v
        else
          attributes[k] = v
        end
      end
      attributes
    end
    
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
      data = signed_parameters(parameters, method.to_s.upcase, DEFAULT_HOST, '/')
      args = if method == :get
        { :query => data }.merge(opts)
      else
        { :body => data }.merge(opts)
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
#      doc.root.namespaces.default_prefix = 'sdb'
      return doc
    end
  end
end