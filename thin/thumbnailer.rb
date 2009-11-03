#!/usr/bin/env ruby

require 'rubygems'
require 'digest/sha2'
require 'fileutils'

# port install freeimage
# gem install image_science
require 'image_science'

# gem install thin
require 'thin'
# gem install em-http-request
require 'em-http'

class DeferrableBody
  include EventMachine::Deferrable

  def call(body)
    body.each do |chunk|
      @body_callback.call(chunk)
    end
  end

  def each &blk
    @body_callback = blk
  end
end

# Implements a thumbnail service for displaying thumbnails based on original images
# stored in S3.  The request comes in like this:
#   http://localhost:3000/t/20090801/1234567890123456789012345678901234567890/630x477-5316.jpg
# We:
#  1) do some sanity checking on the URL, break it into parameters and look for a cached version already generated, 
#     this is all sync and returns immediately if something is wrong.
#  2) async'ly pull the original from S3 using em-http-request.
#  3) if successful, resize the original to the requested size, save it to local disk and return it.
#
# Please note, this example WILL NOT WORK without you setting up an S3 bucket and changing the
# S3HOST constant below.
#
class Thumbnailer
  AsyncResponse = [-1, {}, []].freeze

  S3HOST = "your-image-bucket.s3.amazonaws.com"
  
  def call(env)
    url = env["PATH_INFO"]
    img = image_request_for(url)

    return invalid! unless img
    return invalid! 'Tampered URL' unless img.valid_checksum? or development?(env)

    return [200, 
      {'Content-Type' => content_type(img.extension), 'X-Accel-Redirect' => img.nginx_location}, 
      # If development, send the bytes as the response body in addition to the nginx header, so
      # we can see the image in our local browser.
      development?(env) ? [File.read(img.file_location)] : []] if img.cached?
      
    # We've done all we can synchonously.  Next we need to pull the data from S3 and
    # return a response based on the result.
    EventMachine.next_tick do
      body = DeferrableBody.new
      http = EventMachine::HttpRequest.new("http://#{S3HOST}/#{img.s3_location}").get(:timeout => 5)
      http.errback do
        code = http.response_header.status
        log("Error!!! #{code} #{url}")
        env['async.callback'].call [code, {'Content-Type' => 'text/plain'}, body]
        body.call [http.response]
        body.succeed
      end
      http.callback do
        code = http.response_header.status
        log("Fetched #{url}: #{code}")
        img.to_thumb(http.response)

        # Now that we've recv'd enough data from S3, we can start the async response
        # back to the browser by calling the async callback in Thin.
        env['async.callback'].call [200, {'Content-Type' => content_type(img.extension), 'X-Accel-Redirect' => img.nginx_location}, body]

        # If development, send the bytes as the response body in addition to the nginx header, so
        # we can see the image in our local browser.
        if development?(env)
          body.call [File.read(img.file_location)]
        else
          body.call []
        end
        body.succeed
      end
    end
    AsyncResponse
  end
  
  def content_type(extension)
    case extension
    when 'jpg'
      return 'image/jpeg'
    when 'gif'
      return 'image/gif'
    when 'png'
      return 'image/png'
    else
      return 'image/jpeg'
    end
  end
  
  def invalid!(msg='Invalid URL')
    [400, {"Content-Type" => "text/html"}, [msg]]
  end
  
  def not_found!
    [404, {"Content-Type" => "text/html"}, ["404 Not Found"]]
  end
  
  def internal_error!(msg)
    [500, {"Content-Type" => "text/html"}, ["Internal Error: #{msg}"]]
  end

  def image_request_for(url)
    if url =~ /^\/[a-z]\/(\d{8})\/(\w{40})\/(\d{2,3})x(\d{2,3})-(\w{4}).(\w{3})(?:\?(.*))?$/
      ImageRequest.new($1, $2, $3, $4, $5, $6, $7)
    end
  end

  def development?(env)
    env['SERVER_NAME'] == 'localhost'
  end

  def log(msg)
    puts msg
  end
end

class ImageRequest
  SECRET_SALT = 'hello world'
  THUMB_ROOT = '/tmp/thumbs'
  FileUtils.mkdir_p THUMB_ROOT
  
  attr_accessor :extension

  def initialize(date, image_id, width, height, checksum, extension, params)
    @date = date
    @image_id = image_id
    @width = width
    @height = height
    @checksum = checksum
    @extension = extension
    @params = params
  end
  
  def valid_checksum?
    data = "#{SECRET_SALT}|#{@date}|#{@image_id}|#{@width}|#{@height}"
    code = Digest::SHA2.hexdigest(data)[0..3]
    @checksum == code
  end
  
  def to_thumb(data)
    ImageScience.with_image_from_memory(data) do |img|
      img.resize(Integer(@width), Integer(@height)) do |thumb|
        thumb.save(file_location)
      end
    end
  end
  
  def s3_location
    "#{@image_id}.#{@extension}"
  end
  
  def cached?
    File.exist? file_location
  end
  
  def file_location
    @file_path ||= begin
      dir = "#{THUMB_ROOT}/#{@image_id[0..1]}/#{@image_id[2..3]}"
      FileUtils.mkdir_p dir unless File.directory? dir
      "#{dir}/#{@image_id}-#{@width}x#{@height}.#{@extension}"
    end
  end

  def nginx_location
    file_location.slice(4..-1)
  end

end

if $0 == __FILE__
  Thin::Server.start('0.0.0.0', 3000) do
    use Rack::CommonLogger
    run Thumbnailer.new
  end
end