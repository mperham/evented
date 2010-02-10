# Do your post daemonization configuration here
# At minimum you need just the first line (without the block), or a lot
# of strange things might start happening...
DaemonKit::Application.running! do |config|
  # Trap signals with blocks or procs
  config.trap( 'HUP' ) do
    # Dump the REE stack traces
    p caller_for_all_threads if Object.respond_to? :caller_for_all_threads
  end
  config.trap('TERM') do
    # TODO print out the number of messages waiting to be processed
  end
end

require 'dispatch'

Qanat.run do
  DaemonKit.logger.info "start"

  Fiber.new do
    sqs = SQS::Queue.new(DaemonKit.arguments.options[:queue_name])
    sqs.poll(3) do |msg|
      Fiber.new do 
        Qanat.dispatch(msg)
      end.resume
    end
  end.resume
end


# Images
# IMAGE_SETS = 
#   [
#     ["85740637ed7ac07ce1444845ec02368fa636d395", "1b4c4a534b318a1c447691c5a13c79b3606350da", "02af3198b237091d8b5753ce3412456d75292384", "e27c68aba619350e53de129b70b5976715765854"],
#     ["93cb06618e6c1e3a7fbe68be42e1ee50c995a997", "ca98bfcf1b4b35dcb714701edfacb868984f5761", "a4f11d132853e56aec9817af8d1b0bb733a5e664", "349c788e1dcd16d2f81199b4fbff1dc79ff69eeb"],
#     ["fcf55f50662621c980f1e6d492d053f641744b96", "13f799e03d554c0566473e61a26b7abac9e3d9e0", "a55dbd9926ac7f68a5c9faa8077a0711dd3e0669", "b7b7ee7bcd0b644d21baf352378dab024a8ba297"],
#     ["f7f65b4bb8b9cb0dd5934e5a58dc0fe3f5aaa615", "1f23391992bf661e4d36391f924f322ac568a69d", "3ae7cf197b5722929fabfa8f5b29e12eefba95d1", "3c5465eb47b0bfd0a3d46fe344f8bbeefe613d9e"],
#     ["617fee9c94cc2ddd3f32d519087d08b9a2898cda", "ec1833e79cf9acc66dd0bcc8d1f5e002b5afb128", "7f8ed92c083144d47bc41e762cac8f55efc9dfff", "32b87a0b4e7d99fd30e1652eeb521da6b2e55303"],
#     ["140cec0d609c4a00e093f4f37565e2aba2056c15", "7bb7a6d81d2efea86a085cf18054554852c1af1b", "94445b78c5f715104322de5470b7dbd347f0b7ab", "1fb702a09c7164da7a87580a899cdb96825caec6"],
#     ["3768ec9758996e74417562ebb10e5d9b4ebfdf17", "dd850940107913fd0ea2e8ab439f9e4b72380c4f", "0e1a429fb4847370a5a99a364827b7becd795a2a", "4992808d11c603017af68ae2bca432641b343b85"],
#     ["8748af8ba5e58ae352fecb85e577232ff7f148d6", "dec43a1d45fe6c01ca92d616e46f52415f35b077", "1ec5a121d6a54f04ed47b3972ef7bd75038a2c46", "f2b6de6c8ece6e676a0f92e8fc44e5f3a90a077b"],
#     ["23fefcac4410ce26307ae0cc2975bc9bb32e0cf9", "1e3821afd31b84b839528f7819310d04da1d65df", "e89f004792528e2fa9b6bc2e1179cf3da3806be8", "838b2e0674162956ed41e98c0feb68a91bb42838"],
#     ["be610c0141988b8c03d0e2bad90d3336d80afe8b", "25f4ce7d8662ec83dbe7175d0ce8270f67186327", "788f2ba821200ac9762e443bb84e3f3f814c1285", "7887750e3429354460d850c9c13a9cf370feec62"],
#     ["6e353eebef1a51c9f00854e072c6ce645d0881f1"],
#   ]
# 
# Qanat.run do
#   DaemonKit.logger.info "start"
#   
#   sdb = SDB::Database.new('images-staging')
#   IMAGE_SETS.each_with_index do |images, idx|
#     Fiber.new do
#       images.each do |iid|
#         p [idx, sdb.get(iid)]
#       end
#     end.resume
#   end
#   
#   Fiber.new do
#     sqs = SQS::Queue.new('test')
#   
#     sqs.poll(5) do |msg|
#       DaemonKit.logger.info "Processing #{msg}"
#     
#       # obj = YAML::load(msg)
#       # dispatch(obj, priority)
#     end
#   end.resume
#   
#   s3 = S3::Bucket.new('onespot-test')
#   Fiber.new do
#     s3.put('sqs.rb', File.read(File.dirname(__FILE__) + '/../lib/sqs.rb'))
#     puts s3.get('sqs.rb')
#   end.resume
# end
# 
