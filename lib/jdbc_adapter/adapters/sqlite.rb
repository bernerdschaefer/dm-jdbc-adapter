module DataMapper
  module Adapters
    class JdbcAdapter
      class Sqlite < Abstract

        private
        def generated_keys(connection)
          statement = connection.createStatement
          result_set = statement.executeQuery("select last_insert_rowid()")
          metadata = result_set.getMetaData

          keys = nil

          while result_set.next
            key = jdbc_to_ruby(metadata.getColumnType(1), result_set.getObject(1))
          end

          result_set.close
          statement.close

          key == 0 ? nil : key
        end

      end # class Sqlite
    end # class JdbcAdapter
  end # module Adapters
end # module DataMapper