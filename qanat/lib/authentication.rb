module Amazon
  module Authentication
    SIGNATURE_VERSION = "2"
    @@digest = OpenSSL::Digest::Digest.new("sha256")

    def sign(auth_string)
      Base64.encode64(OpenSSL::HMAC.digest(digester, aws_secret_access_key, auth_string)).strip
    end
    
    def digester
      @@digest
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

    def signed_parameters(hash, verb, host, path)
      data = hash.keys.sort.map do |key|
        "#{amz_escape(key)}=#{amz_escape(hash[key])}"
      end.join('&')
      sig = amz_escape(sign("#{verb}\n#{host}\n#{path}\n#{data}"))
      "#{data}&Signature=#{sig}"
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
end