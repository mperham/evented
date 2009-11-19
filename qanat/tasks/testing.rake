require 'right_aws'
require 'qanat'

namespace :msg do
  task :push do
    hash = Qanat.load('sqs')
    
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
end
