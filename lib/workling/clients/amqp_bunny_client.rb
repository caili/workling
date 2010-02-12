require 'workling/clients/base'
Workling.try_load_an_amqp_client

require 'bunny'

#
#  An Ampq client
#
module Workling
  module Clients
    class AmqpBunnyClient < Workling::Clients::Base
      
      def new_bunny
        b = Bunny.new()
        b.start
        b
      end
    
#      def bunny
#        @bunny ||= new_bunny
#      end

      # starts the client. 
      def connect
        begin
          @amq = MQ.new
        rescue
          @bunny ||= new_bunny
#          raise WorklingError.new("couldn't start amq client. if you're running this in a server environment, then make sure the server is evented (ie use thin or evented mongrel, not normal mongrel.)")
        end
      end
      
      # no need for explicit closing. when the event loop
      # terminates, the connection is closed anyway. 
      def close; true; end
      
      # subscribe to a queue
      def subscribe(key)
        @amq.queue(key).subscribe do |data|
          value = Marshal.load(data)
          yield value
        end
      end
      
      # request and retrieve work
      def retrieve(key); @amq.queue(key); end
      def request(key, value)
        data = Marshal.dump(value)
#        @amq.queue(key).publish(data)
        @bunny.queue(key).publish(data)
      end
    end
  end
end
