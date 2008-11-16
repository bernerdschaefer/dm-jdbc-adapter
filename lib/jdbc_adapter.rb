require "rubygems"
require "pathname"
require "dm-core"

module DataMapper
  module Adapters
    class JdbcAdapter < AbstractAdapter

      def execute(statement, *bind_values)

        with_connection do |connection|
          result = nil
          success = false
          metadata = connection.getMetaData

          # DataMapper.logger.debug(statement + "\n -- " + bind_values.inspect)

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
              hash[metadata.getColumnName(i)] = jdbc_to_ruby(rs.getObject(i))
            end
            result << hash
          end

          result
        end
      end

      def create(resources)
        created = 0

        with_connection do |connection|
          resources.each do |resource|
            repository = resource.repository
            model = resource.model
            attributes = resource.dirty_attributes

            statement = "INSERT INTO #{model.storage_name(repository.name)} ("
            statement << attributes.keys.map { |property| property.field(repository.name) } * ", "
            statement << ") VALUES ("
            statement << (['?'] * attributes.size) * ", "
            statement << ")"

            bind_values = attributes.values

            result = execute(statement, *bind_values)
            created += 1 if result

          end
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

      protected

      def normalize_uri(uri)
        JdbcUri.new(uri)
      end

      private

      def initialize(name, uri_or_options)
        require Pathname(__FILE__).dirname + "jdbc_adapter/jdbc_uri"

        super

        require @uri.gem

        # Touch the driver class
        import @uri.driver

        # Require and include the DB specific extensions
        require Pathname(__FILE__).dirname + "jdbc_adapter/adapters/#{@uri.scheme}"
        self.class.send(:include, self.class.const_get(Extlib::Inflection.classify(@uri.scheme)))
      end

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

      def ruby_to_jdbc(ruby_object)
        case ruby_object
        when String, Integer, Float, nil then ruby_object
        else ruby_object
        end
      end

      def jdbc_to_ruby(jdbc_object)
        case jdbc_object
        when String, Integer, Float, nil then jdbc_object
        else raise("Unsupported JDBC Type")
        end
      end

      # def generated_keys(connection, result_set = nil)
      #   raise NotImplementedError
      # end

    end
  end
end