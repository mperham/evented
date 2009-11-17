# Your starting point for daemon specific classes. This directory is
# already included in your load path, so no need to specify it.

module Qanat
  def self.load(config)
    config = config.to_s
    config += '.yml' unless config =~ /\.yml$/

    hash = {}
    path = File.join( DAEMON_ROOT, 'config', config )
    hash.merge!(YAML.load_file( path )) if File.exists?(path)

    path = File.join( ENV['HOME'], ".qanat.#{config}" )
    hash.merge!(YAML.load_file( path )) if File.exists?(path)
    
    raise ArgumentError, "Can't find #{path}" if hash.size == 0

    hash[DAEMON_ENV]
  end
end
    