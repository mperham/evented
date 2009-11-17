require 'right_aws'

namespace :msg do
  task :push do
    config = YAML::load(File.read(File.dirname(__FILE__) + '/../config/amzn.yml'))
    
    ACCESS_KEY = config['test']['access_key']
    SECRET_KEY = config['test']['secret_key']
    queue = ENV['QUEUE'] || 'test'
    count = Integer(ENV['COUNT'] || '5')
    sqs = RightAws::SqsGen2.new(ACCESS_KEY, SECRET_KEY, :protocol => 'http', :port => 80)
    q = sqs.queue(queue)
    count.times do
      q.push('test message')
    end
  end
end
