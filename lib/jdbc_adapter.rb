require "rubygems"
require "pathname"
require "dm-core"

module DataMapper
  module Adapters
    class JdbcAdapter < AbstractAdapter

      def execute(statement, *bind_values)
        with_connection do |connection|
          if bind_values.empty?
            stmt = connection.createStatement
            result = stmt.execute(statement)
          end
        end
      end

      def query(statement, *bind_values)
        with_connection do |connection|
          result = []
          if bind_values.empty?
            stmt = connection.createStatement
            rs = stmt.executeQuery(statement)
          else
          end
          metadata = rs.getMetaData

          while rs.next do
            hash = {}
            1.upto(metadata.getColumnCount) do |i|
              hash[metadata.getColumnName(i)] = rs.getObject(i)
            end
            result << hash
          end

          result
        end
      end

      def create(resources)
        raise NotImplementedError
      end

      def read_many(query)
        raise NotImplementedError
      end

      def read_one(query)
        raise NotImplementedError
      end

      def update(attributes, query)
        raise NotImplementedError
      end

      def delete(query)
        raise NotImplementedError
      end

      protected

      def normalize_uri(uri)
        JdbcUri.new(uri)
      end

      private

      def initialize(name, uri_or_options)
        super

        require @uri.gem

        # Touch the driver class
        import @uri.driver
      end

      def with_connection(&block)
        connection = nil
        begin
          connection = java.sql.DriverManager.getConnection(@uri.to_s)
          return yield(connection)
        rescue => e
          DataMapper.logger.error(e)
          puts e
          puts e.backtrace
          raise e
        ensure
          connection.close if connection
        end
      end

      class JdbcUri
        def initialize(uri)
          @uri = Addressable::URI.parse(uri.path)
        end

        def gem
          case @uri.scheme
          when "sqlite" then "jdbc/sqlite3"
          when "mysql"  then "jdbc/mysql"
          end
        end

        def driver
          case @uri.scheme
          when "sqlite" then "org.sqlite.JDBC"
          when "mysql"  then "com.mysql.jdbc.Driver"
          end
        end

        def to_s
          "jdbc:#{@uri.to_s}"
        end
      end

    end
  end
end