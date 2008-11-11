require File.dirname(__FILE__) + "/helper"

include DataMapper::Adapters

describe "JdbcAdapter" do
  before(:all) do
    @database = (Pathname(__FILE__).dirname.expand_path + "test.db").to_s
    @adapter = JdbcAdapter.new(:default, Addressable::URI.parse("jdbc:sqlite:#{@database}"))
  end

  after(:all) do
    File.unlink(@database)
  end

  describe "#execute" do
    it "should execute a query" do
      @adapter.execute("CREATE TABLE users (id integer primary key, name varchar)")
    end
  end

  describe "#query" do
    before(:all) do
      @adapter.execute("CREATE TABLE users (id integer primary key, name varchar)")
      @adapter.execute("INSERT INTO users (name) VALUES ('John')")
    end

    it "should return a result set" do
      result = @adapter.query("SELECT * FROM users")
      result.first.should == { "id" => 1, "name" => "John" }
    end
  end
end