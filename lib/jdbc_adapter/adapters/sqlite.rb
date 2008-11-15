module DataMapper
  module Adapters
    class JdbcAdapter
      module Sqlite

        def generated_keys(connection)
          statement = connection.createStatement
          result_set = statement.executeQuery("select last_insert_rowid()")

          keys = nil

          while result_set.next
            key = jdbc_to_ruby(result_set.getObject(1))
          end

          result_set.close
          statement.close

          key == 0 ? nil : key
        end

      end # module Sqlite
    end # class JdbcAdapter
  end # module Adapters
end # module DataMapper