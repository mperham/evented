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
  
  concurrency = 10
  local = EM::Queue.new
  sqs = SQS::Queue.new('test')
  
  process_one = proc { |msg|
    DaemonKit.logger.info "process_one"
    process(msg) {
      pop_next
    }
  }

  pop_next = proc {
    DaemonKit.logger.info "pop_next"
    local.pop(process_one)
  }

  concurrency.times { pop_next }
  
  recharge = proc {
    DaemonKit.logger.info "recharge"
    sqs.receive_msg(concurrency) do |msg|
      DaemonKit.logger.info "pushing to local #{local.size}"
      local.push(msg)
    end
  }

  recharge.call
  EM.add_periodic_timer(30, &recharge)
  
end

def process(msg)
  DaemonKit.logger.info "Processing #{msg}"
  yield
end