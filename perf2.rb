require 'dalli'
x = ARGV[0]
if x == nil
  x = 300000
else
  x = x.to_i
end
start = Time.now.to_f
options = { }
dc = Dalli::Client.new('localhost:3333', options)
puts dc.set("Hello", "World")
x.times do
dc.get("Hello")
end
puts "#{x} GETs: ", Time.now.to_f - start
