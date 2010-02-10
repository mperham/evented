require 'right_aws'
require 'qanat'

# Some sample Rake tasks to perform common queue tasks.
namespace :msg do
  task :push do
    hash = Qanat.load('amzn')
    
    ACCESS_KEY = hash['access_key']
    SECRET_KEY = hash['secret_key']
    queue = ENV['QUEUE'] || 'test'
    count = Integer(ENV['COUNT'] || '5')
    sqs = RightAws::SqsGen2.new(ACCESS_KEY, SECRET_KEY, :protocol => 'http', :port => 80)
    q = sqs.queue(queue)
    count.times do
      q.push(Time.now.to_s)
    end
  end

  task :clone do
    hash = Qanat.load('amzn')
    
    ACCESS_KEY = hash['access_key']
    SECRET_KEY = hash['secret_key']
    to = ENV['TO'] || 'images'
    from = ENV['FROM'] || 'tasks_production_lowest'
    count = Integer(ENV['COUNT'] || '10')
    sqs = RightAws::SqsGen2.new(ACCESS_KEY, SECRET_KEY, :protocol => 'http', :port => 80)
    from_q = sqs.queue(from)
    to_q = sqs.queue(to)
    msgs = from_q.receive_messages(count)
    raise RuntimeError, "No messages recv'd from #{from}" if msgs.size == 0
    msgs.each do |msg|
      p msg.body
      next if msg.body !~ /crawl_images/
      to_q.push(msg.body)
    end
  end
end
