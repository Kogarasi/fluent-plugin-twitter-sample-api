module Fluent
  class TwitterSearchInput < Fluent::Input
    Fluent::Plugin.register_input('twitter_search', self)

    config_param :consumer_key, :string
	config_param :consumer_secret, :string
	config_param :oauth_token, :string
	config_param :oauth_token_secret, :string
	config_param :tag, :string

    def initialize
	  super
	  require 'tweetstream'
    end

    def configure(conf)
	  super

	  TweetStream.configure do |config|
	    config.consumer_key = @consumer_key
		config.consumer_secret = @consumer_secret
		config.oauth_token = @oauth_token
		config.oauth_token_secret = @oauth_token_secret
		config.auth_method = :oauth
      end
	end

    def start
	  @thread = Thread.new(&method(:run))
	  @any = Proc.new do |hash|
	    state = is_message?(hash)
	    get_message(hash) if state
	  end
	end

	def run
	
      client = TweetStream::Client.new
	  client.on_anything(&@any)
	  client.on_error do |message|
	    $log.info "twitter: #{message}"
	  end
	  client.sample
	end

    def shutdown
	  Thread.kill( @thread )
	end

	def is_message?(status)
	  return false if (!status.include?(:text) )
	  return false if (!status.include?(:user) )

	  return true
	end

	def get_message(status)
	  record = Hash.new
	  record.store( 'message', status[:text].force_encoding('utf-8').encode('utf-8') )
	  record.store( 'geo', status[:geo] )
	  record.store( 'place', status[:place] )
	  record.store( 'created_at', status[:created_at] )
	  record.store( 'user_name', status[:user][:name] )
	  record.store( 'user_screen_name', status[:user][:screen_name] )
	  record.store( 'user_profile_image_url', status[:user][:profile_image_url] )
	  record.store( 'user_time_zone', status[:user][:timezone] )
	  record.store( 'user_lang', status[:user][:lang] )
      Engine.emit( @tag, Engine.now, record )
	end
  end
end
