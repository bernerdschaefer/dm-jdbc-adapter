require File.dirname(__FILE__) + "/helper"

include DataMapper::Adapters

describe "JdbcAdapter" do
  before(:all) do
    @database = (Pathname(__FILE__).dirname.expand_path + "test.db").to_s
    @adapter = JdbcAdapter.new(:default, Addressable::URI.parse("jdbc:sqlite:#{@database}"))
    @adapter.execute <<-SQL
    CREATE TABLE users
      (id integer primary key autoincrement, name varchar(255), age integer, weight float, created_at timestamp)
    SQL
  end

  after(:all) do
    File.unlink(@database)
  end

  describe "ruby_to_jdbc" do

    before(:each) do
      @adapter.execute("DELETE FROM users")
    end

    it "should handle strings" do
      @adapter.execute("INSERT INTO users (name) VALUES (?)", "John")
      @adapter.query("SELECT name FROM users").size.should == 1
    end

    it "should handle integers" do
      @adapter.execute("INSERT INTO users (age) VALUES (?)", 23)
      @adapter.query("SELECT age FROM users").size.should == 1
    end

    it "should handle floats" do
      lambda { @adapter.execute("INSERT INTO users (weight) VALUES (?)", 123.4) }.should_not raise_error
    end

  end

end