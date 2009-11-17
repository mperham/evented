#set :deploy_to, "/svc/qanat" # defaults to "/u/apps/#{application}"
#set :user, ""            # defaults to the currently logged in user
set :daemon_env, 'production'

set :domain, 'example.com'
server domain
