require "open-uri"
require "stringio"
require "fileutils"
require "open3"
require 'tempfile'

module EventedMagick
  class MiniMagickError < RuntimeError; end

  class Image
    attr :path
    attr :tempfile
    attr :output

    # Class Methods
    # -------------
    class << self
      def from_blob(blob, ext = nil)
        begin
          tempfile = Tempfile.new(['evented_magick', ext.to_s])
          tempfile.binmode
          tempfile.write(blob)
        ensure
          tempfile.close if tempfile
        end

        return self.new(tempfile.path, tempfile)
      end

      # Use this if you don't want to overwrite the image file
      def open(image_path)
        File.open(image_path, "rb") do |f|
          self.from_blob(f.read, File.extname(image_path))
        end
      end
      alias_method :from_file, :open
    end

    # Instance Methods
    # ----------------
    def initialize(input_path, tempfile=nil)
      @path = input_path
      @tempfile = tempfile # ensures that the tempfile will stick around until this image is garbage collected.
      @method = defined?(::EM) && EM.reactor_running? ? :evented_execute : :blocking_execute

      # Ensure that the file is an image
      output = run_command("identify", "-format", format_option("%m %w %h"), @path)
      (format, width, height) = output.split
      @values = { 'format' => format, 'width' => width.to_i, 'height' => height.to_i, 'dimensions' => [width.to_i, height.to_i] }
    end

    # For reference see http://www.imagemagick.org/script/command-line-options.php#format
    def [](value)
      key = value.to_s
      return @values[key] if %w(format width height dimensions).include? key
      if key == "size"
        File.size(@path)
      else
        run_command('identify', '-format', "\"#{key}\"", @path).split("\n")[0]
      end
    end

    # Sends raw commands to imagemagick's mogrify command. The image path is automatically appended to the command
    def <<(*args)
      run_command("mogrify", *args << @path)
    end

    # This is a 'special' command because it needs to change @path to reflect the new extension
    # Formatting an animation into a non-animated type will result in ImageMagick creating multiple
    # pages (starting with 0).  You can choose which page you want to manipulate.  We default to the
    # first page.
    def format(format, page=0)
      run_command("mogrify", "-format", format, @path)

      old_path = @path.dup
      @path.sub!(/(\.\w+)?$/, ".#{format}")
      File.delete(old_path) unless old_path == @path

      unless File.exists?(@path)
        begin
          FileUtils.copy_file(@path.sub(".#{format}", "-#{page}.#{format}"), @path)
        rescue e
          raise MiniMagickError, "Unable to format to #{format}; #{e}" unless File.exist?(@path)
        end
      end
    ensure
      Dir[@path.sub(/(\.\w+)?$/, "-[0-9]*.#{format}")].each do |fname|
        File.unlink(fname)
      end
    end

    # Writes the temporary image that we are using for processing to the output path
    def write(output_path)
      FileUtils.copy_file @path, output_path
      run_command "identify", output_path # Verify that we have a good image
    end

    # Give you raw data back
    def to_blob
      f = File.new @path
      f.binmode
      f.read
    ensure
      f.close if f
    end

    # If an unknown method is called then it is sent through the morgrify program
    # Look here to find all the commands (http://www.imagemagick.org/script/mogrify.php)
    def method_missing(symbol, *args)
      args.push(@path) # push the path onto the end
      run_command("mogrify", "-#{symbol}", *args)
      self
    end

    # You can use multiple commands together using this method
    def combine_options(&block)
      c = CommandBuilder.new
      block.call c
      run_command("mogrify", *c.args << @path)
    end

    # Check to see if we are running on win32 -- we need to escape things differently
    def windows?
      !(RUBY_PLATFORM =~ /win32/).nil?
    end

    # Outputs a carriage-return delimited format string for Unix and Windows
    def format_option(format)
      windows? ? "#{format}\\n" : "#{format}\\\\n"
    end

    def run_command(command, *args)
      full_args = args.collect do |arg|        
        # args can contain characters like '>' so we must escape them, but don't quote switches
        if arg !~ /^[\+\-]/
          "\"#{arg}\""
        else
          arg.to_s
        end
      end.join(' ')

      full_cmd = "#{command} #{full_args}"
      (output, status) = send(@method, full_cmd)

      if status.exitstatus == 0
        output
      else
        raise MiniMagickError, "ImageMagick command (#{full_cmd.inspect}) failed: #{{:status_code => status, :output => output}.inspect}"
      end
    end

    def evented_execute(cmd)
      fiber = Fiber.current
      EM::system(cmd) do |output, status|
        fiber.resume([output, status])
      end
      
      Fiber.yield
    end
    
    def blocking_execute(cmd)
      output = `#{cmd}`
      [output, $?]
    end
  end

  class CommandBuilder
    attr :args

    def initialize
      @args = []
    end

    def method_missing(symbol, *args)
      @args << "-#{symbol}"
      @args += args
    end

    def +(value)
      @args << "+#{value}"
    end
  end
end
