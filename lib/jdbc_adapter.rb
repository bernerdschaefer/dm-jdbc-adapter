require "rubygems"
require "pathname"
require "dm-core"

require Pathname(__FILE__).dirname + "jdbc_adapter/jdbc_uri"
require Pathname(__FILE__).dirname + "jdbc_adapter/adapters/abstract"

module DataMapper
  module Adapters
    class JdbcAdapter

      ##
      # Returns an instance of the database specific adapter.
      # 
      def self.new(name, uri)
        uri = JdbcUri.new(uri)
        require Pathname(__FILE__).dirname + "jdbc_adapter/adapters/#{uri.scheme}"

        return const_get(Extlib::Inflection.classify(uri.scheme)).new(name, uri)
      end

    end
  end
end