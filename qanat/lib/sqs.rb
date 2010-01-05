module SQS
  DEFAULT_HOST = URI.parse("http://queue.amazonaws.com/")
  API_VERSION = "2009-02-01"

  class Queue
    REQUEST_TTL = 30
    
    include Amazon::Authentication

    def initialize(name)
      @config = Qanat.load('amzn')
      @uri = URI.parse(url_for(name))
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
    
    def push(msg)
      request_hash = generate_request_hash("SendMessage", 'MessageBody' => msg)
      http = async_operation(:get, @uri, request_hash, :timeout => timeout)
      code = http.response_header.status
      if code != 200
        logger.error "SQS send_message returned an error response: #{code} #{http.response}"
      end
    end
    
    private
    
    def create(name)
      request_hash = generate_request_hash("CreateQueue", 'QueueName' => name)
      http = async_operation(:post, DEFAULT_HOST, request_hash, :timeout => timeout)
      code = http.response_header.status
      if code != 200
        logger.error "SQS send_message returned an error response: #{code} #{http.response}"
      end
    end
    
    def url_for(name, recur=false)
      raise ArgumentError, "No queue given" if !name || name.strip == '' 
      request_hash = generate_request_hash("ListQueues", 'QueueNamePrefix' => name)
      http = async_operation(:get, DEFAULT_HOST, request_hash, :timeout => timeout)
      code = http.response_header.status
      if code == 200
        doc = Nokogiri::XML(http.response)
        tag = doc.xpath('//xmlns:QueueUrl').first
        if !tag
          if !recur
            create(name)
            return url_for(name, true)
          else
            raise ArgumentError, "Unable to create queue '#{name}'"
          end
        end
        url = tag.content
        logger.info "Queue #{name} at #{url}"
        return url
      end
    end
    
    def delete_msg(handle)
      logger.info "Deleting #{handle}"
      request_hash = generate_request_hash("DeleteMessage", 'ReceiptHandle' => handle)
      http = async_operation(:get, @uri, request_hash, :timeout => timeout)
      code = http.response_header.status
      if code != 200
        logger.error "SQS delete returned an error response: #{code} #{http.response}"
      end
    end
    
    def receive_msg(count=1, &block)
      request_hash = generate_request_hash("ReceiveMessage", 'MaxNumberOfMessages'  => count,
          'VisibilityTimeout' => 600)
      http = async_operation(:get, @uri, request_hash, :timeout => timeout)
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
          logger.info "Queue #{@uri} is empty"
          Fiber.sleep(5)
        end
      else
        logger.error "SQS returned an error response: #{code} #{http.response}"
        Fiber.sleep(5)
        # TODO parse the response and print something useful
        # TODO retry a few times with exponentially increasing delay
      end
    end
    
    def async_operation(method, uri, parameters, opts)
      f = Fiber.current
      data = signed_parameters(parameters, method.to_s.upcase, uri.host, uri.path)
      args = if method == :get
        { :query => data }.merge(opts)
      else
        { :body => data }.merge(opts)
      end
      http = EventMachine::HttpRequest.new(uri).send(method, args)
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