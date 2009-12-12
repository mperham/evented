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

SQS.run do
  
  DaemonKit.logger.info "start"
  
  sqs = SQS::Queue.new('test')
  sqs.poll(5) do |msg|
    sec = rand(10) + 1
    DaemonKit.logger.info "Processing #{msg} for #{sec} seconds"
    fiber_sleep(sec)
  end
  
end

