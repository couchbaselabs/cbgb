#!/usr/bin/env ruby

h = ARGV[1] || "http://127.0.0.1:8077"
n = (ARGV[0] || "200").to_i
n.times.each do |i|
  bucketName = "b" + i.to_s.rjust(4, '0')
  p "bucketName #{bucketName}"
  p `curl -X POST #{h}/_api/buckets -F bucketName=#{bucketName}`
end

