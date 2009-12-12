# Do your post daemonization configuration here
# At minimum you need just the first line (without the block), or a lot
# of strange things might start happening...
DaemonKit::Application.running! do |config|
  # Trap signals with blocks or procs
  # config.trap( 'INT' ) do
  #   # do something clever
  # end
  # config.trap( 'TERM', Proc.new { puts 'Going down' } )
end

require 'sqs'
require 'simpledb'

def dispatch(msg, priority)
  notify_upon_exception('jobber', msg) do |hash|
    name = hash.fetch(:msg_type).to_s.camelize
    profile hash.inspect do
      name.constantize.new.process(hash, priority)
    end
  end
end

def notify_upon_exception(name, ctx)
  return yield(ctx) if Rails.env == 'test'

  begin
    yield ctx
  rescue => exception
    DaemonKit.logger.info "Exception: #{exception.message}"
    DaemonKit.logger.info exception.backtrace.join("\n")
  end
end


SQS.run do
  
  DaemonKit.logger.info "start"
  
  sqs = SQS::Queue.new('test')
  sqs.poll(5) do |msg|
    DaemonKit.logger.info "Processing #{msg}"
    obj = YAML::load(msg)
    dispatch(obj, priority)
  end
  
end

