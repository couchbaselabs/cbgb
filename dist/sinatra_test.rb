# VERY simple way to web-serve up a folder for local testing purposes
# if you have Ruby installed, and Sinatra gem, simply $ ruby sinatra_test.rb and go to http://localhost:4567/

require 'rubygems'
require 'sinatra'

get '/' do
    send_file File.expand_path("index.html", ".")
end

get '/*' do |path|

    puts "requested #{path}"
    puts

    send_file File.expand_path("./" + path)
end

