require "pathname"
require Pathname(__FILE__).dirname.parent + "lib/jdbc_adapter"

require "spec"

class User
  include DataMapper::Resource

  property :id, Serial
  property :name, String

end