module DataMapper
  module Adapters
    class JdbcAdapter
      module Mysql

        def generated_keys(connection, result_set)

          puts "HERE?"
          key = nil

          result_set.next
          key = jdbc_to_ruby(result_set.getObject(1))
          result_set.close

          key == 0 ? nil : key
        end

      end # module Sqlite
    end # class JdbcAdapter
  end # module Adapters
end # module DataMapper