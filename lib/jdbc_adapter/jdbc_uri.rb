module DataMapper
  module Adapters
    class JdbcAdapter
      class JdbcUri
        def initialize(uri)
          @uri = Addressable::URI.parse(uri.path)
        end

        def scheme
          return @uri.scheme
        end

        def gem
          case scheme
          when "sqlite" then "jdbc/sqlite3"
          when "mysql"  then "jdbc/mysql"
          end
        end

        def driver
          case scheme
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