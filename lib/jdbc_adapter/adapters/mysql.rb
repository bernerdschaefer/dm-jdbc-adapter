module DataMapper
  module Adapters
    class JdbcAdapter
      module Mysql

        def generated_keys(connection, statement = nil)

          return nil unless statement

          result_set = statement.getGeneratedKeys
          key = nil

          while result_set.next
            key = jdbc_to_ruby(result_set.getObject(1))
          end
          result_set.close

          key == 0 ? nil : key
        end

      end # module Sqlite
    end # class JdbcAdapter
  end # module Adapters
end # module DataMapper