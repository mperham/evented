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
  local = EM::Queue.new
  sqs = SQS::Queue.new('test')
  
  concurrency = 10
  process_one = proc{ |msg|
    process(msg) {
      pop_next
    }
  }

  pop_next = proc {
    local.pop(process_one)
    sqs.receive_msg do |msg|
      local.push(msg)
    end
  }

  concurrency.times {
    sqs.receive_msg do |msg|
      local.push(msg)
      pop_next
    end
  }
  
  recharge = proc { |msg|
    local.push(msg)
    pop_next
  }
  
  EM.add_periodic_timer(30) do
    (concurrency - local.size).times do
      sqs.receive_msg(&recharge)
    end
  end
end

def process(msg)
  DaemonKit.logger.info "Processing #{msg}"
end