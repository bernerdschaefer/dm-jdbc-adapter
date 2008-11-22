module DataMapper
  module Adapters
    class JdbcAdapter
      class Abstract < AbstractAdapter
        def execute(statement, *bind_values)

          with_connection do |connection|
            result = nil
            success = false
            metadata = connection.getMetaData

            DataMapper.logger.debug(statement + "\n -- " + bind_values.inspect)

            if bind_values.empty?
              stmt = connection.createStatement
              if metadata.supportsGetGeneratedKeys
                stmt.executeUpdate(statement)
                result = generated_keys(connection, stmt)
              else
                stmt.execute(statement)
                result = generated_keys(connection)
              end
              stmt.close
            else
              if metadata.supportsGetGeneratedKeys
                stmt = connection.prepareStatement(statement, 1)
              else
                stmt = connection.prepareStatement(statement)
              end

              bind_values.each_with_index do |bind_value, i|
                stmt.setObject(i + 1, ruby_to_jdbc(bind_value))
              end

              stmt.execute

              if metadata.supportsGetGeneratedKeys
                result = generated_keys(connection, stmt)
              else
                result = generated_keys(connection)
              end

              stmt.close
            end

            result
          end

        end

        def query(statement, *bind_values)
          with_connection do |connection|
            result = []
            if bind_values.empty?
              stmt = connection.createStatement
              rs = stmt.executeQuery(statement)
            else
              stmt = connection.prepareStatement(statement)
              bind_values.each_with_index do |bind_value, i|
                stmt.setObject(i + 1, ruby_to_jdbc(bind_value))
              end
              rs = stmt.executeQuery
            end

            metadata = rs.getMetaData

            while rs.next do
              hash = {}
              1.upto(metadata.getColumnCount) do |i|
                hash[metadata.getColumnName(i)] = jdbc_to_ruby(metadata.getColumnType(i), rs.getObject(i))
              end
              result << hash
            end

            result
          end
        end

        def create(resources)
          created = 0

          resources.each do |resource|
            repository = resource.repository
            model = resource.model
            attributes = resource.dirty_attributes

            statement = "INSERT INTO #{quote_identifier(model.storage_name(repository.name))} ("
            statement << attributes.keys.map { |property| quote_identifier(property.field(repository.name)) } * ", "
            statement << ") VALUES ("
            statement << (['?'] * attributes.size) * ", "
            statement << ")"

            bind_values = attributes.values

            result = execute(statement, *bind_values)
            created += 1 if result

          end

          created
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

        private

        def initialize(name, uri)
          super

          # Require the driver gem
          require @uri.gem

          # Touch the driver class
          import @uri.driver
        end

        ##
        # Yield a connection from the DriverManager, catching errors
        # and logging them, and ensuring that the connection closes.
        # 
        def with_connection(&block)
          connection = nil
          begin
            connection = java.sql.DriverManager.getConnection(@uri.to_s)
            return yield(connection)
          rescue => e
            DataMapper.logger.error(e.to_s)
            puts e
            raise e
          ensure
            connection.close if connection
          end
        end

        ##
        # Convert Ruby object to JDBC/Java object.
        # 
        def ruby_to_jdbc(ruby_object)
          case ruby_object
          when String, Integer, Float, nil then ruby_object
          else ruby_object
          end
        end

        ##
        # Convert JDBC/Java object to Ruby object.
        # 
        def jdbc_to_ruby(type, jdbc_object)
          case jdbc_object
          when String, Integer, Float, nil then jdbc_object
          else jdbc_object
          end
        end

        ##
        # Retrieve the string user for quoting table and column names for this
        # connection. Default to '"' if the string returned by the connection
        # does not specify a character.
        # 
        def quote_string
          @quote_string ||= with_connection { |connection| connection.getMetaData.getIdentifierQuoteString }
          @quote_string = '"' if @quote_string == " "
          @quote_string
        end

        ##
        # Quotes the table or column name according the connection's declared
        # quote string.
        # 
        def quote_identifier(identifier)
          identifier.gsub(/([^\.]+)/, "#{self.quote_string}\\1#{self.quote_string}")
        end

        ##
        # Returns the generated keys for database drivers which support returning
        # the keys through the JDBC (DatabaseMetaData.supportsGetGeneratedKeys).
        # 
        # Drivers which do not support this (like Sqlite) overwrite this function
        # to run a query to retrieve the key.
        # 
        def generated_keys(connection, statement = nil)

          return nil unless statement

          result_set = statement.getGeneratedKeys
          metadata = result_set.getMetaData
          key = nil

          while result_set.next
            key = jdbc_to_ruby(metadata.getColumnType(1), result_set.getObject(1))
          end
          result_set.close

          key == 0 ? nil : key
        end
      end
    end
  end
end