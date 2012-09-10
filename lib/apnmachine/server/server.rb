module ApnMachine
  module Server
    class Server
      def initialize(pem, pem_passphrase = nil, redis_host = '127.0.0.1', redis_port = 6379, redis_uri = nil, apn_host = 'gateway.push.apple.com', apn_port = 2195, log = '/apnmachined.log')
        @pem, @pem_passphrase, @apn_host, @apn_port = pem, pem_passphrase, apn_host, apn_port

        if redis_uri
          uri = URI.parse(redis_uri)
          @redis = Redis.new(:host => uri.host, :port => uri.port, :password => uri.password)
        else
          @redis = Redis.new(:host => redis_host, :port => redis_port)
        end

        #set logging options
        if log == STDOUT
          Config.logger = Logger.new STDOUT
        elsif File.exist?(log)
          @flog = File.open(log, File::WRONLY | File::APPEND)
          Config.logger = Logger.new(@flog, 'daily')
        else
          require 'fileutils'
          FileUtils.mkdir_p(File.dirname(log))
          @flog = File.open(log, File::WRONLY | File::APPEND | File::CREAT)
          Config.logger = Logger.new(@flog, 'daily')
        end
      end

      def connect!
        raise "The path to your pem file is not set." unless @pem
        raise "The path to your pem file does not exist!" unless File.exist?(@pem)

        @context = OpenSSL::SSL::SSLContext.new
        @context.cert = OpenSSL::X509::Certificate.new(File.read(@pem))
        @context.key = OpenSSL::PKey::RSA.new(File.read(@pem), @pem_passphrase)

        @socket = TCPSocket.new(@apn_host, @apn_port)
        @ssl_socket = OpenSSL::SSL::SSLSocket.new(@socket, @context)
        @ssl_socket.sync = true
        @ssl_socket.connect

        Config.logger.info "Connection to Apple Servers completed"
      end

      def start!
        Config.logger.info "Connecting to Apple Servers"

        connect!

        Config.logger.info "Starting APN Server on Redis"

        loop do
          notification = @redis.lpop("apnmachine.queue")

          if notification
            retries = 3

            begin
              #prepare notification
              notif_bin = Notification.to_bytes(notification)

              #sending notification
              Config.logger.debug 'Sending notification to APN'
              @ssl_socket.write(notif_bin)
              Config.logger.debug 'Notification sent'
            rescue Errno::EPIPE, OpenSSL::SSL::SSLError, Errno::ECONNRESET, Errno::ETIMEDOUT
              if retries > 1
                Config.logger.error "Connection to APN servers idle for too long. Trying to reconnect"

                sleep 2
                connect!

                retries -= 1
                retry
              else
                Config.logger.error "Can't reconnect to APN Servers! Saving notification to the queue #{notification.to_s}"
                @redis.rpush("apnmachine.queue", notification)

                Config.logger.error "Attempting to reconnect in 30 seconds"
                sleep 30
                connect!
              end
            rescue Exception => e
              Config.logger.error "Unable to handle: #{e}"
            end #end of begin
          else
            sleep 1
          end # if notification

          @flog.flush if @flog
        end #end of loop
      end # def start!
    end #class Server
  end #module Server
end #module ApnMachine
