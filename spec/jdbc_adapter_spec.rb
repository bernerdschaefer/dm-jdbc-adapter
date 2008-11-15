require File.dirname(__FILE__) + "/helper"

include DataMapper::Adapters

describe "JdbcAdapter" do
  before(:all) do
    case ENV["ADAPTER"]
    when "mysql"
      # `mysqladmin -uroot create jdbc_test`
      @adapter = DataMapper.setup(:default, "jdbc:mysql://127.0.0.1:3306/jdbc_test?user=root")
    else
      @database = (Pathname(__FILE__).dirname.expand_path + "test.db").to_s
      @adapter = DataMapper.setup(:default, "jdbc:sqlite:#{@database}")
    end
  end

  after(:all) do
    case ENV["ADAPTER"]
    when "mysql"
      # `mysqladmin -f -uroot drop jdbc_test`
    else
      File.unlink(@database)
    end
  end

  describe "#execute" do
    it "should execute a query" do
      @adapter.execute("CREATE TABLE users (id integer primary key auto_increment, name varchar(255))")
    end

    it "should accept bind values" do
      @adapter.execute("INSERT INTO users (name) VALUES (?)", "John")
    end

    it "should return generated keys when present" do
      keys = @adapter.execute("INSERT INTO users (name) VALUES(?)", "John")
      keys.should == 2
    end
  end # execute

  describe "#query" do
    before(:all) do
      @adapter.execute("CREATE TABLE users (id integer primary key auto_increment, name varchar(255))")
      @adapter.execute("INSERT INTO users (name) VALUES ('John')")
    end

    it "should return a result set" do
      result = @adapter.query("SELECT * FROM users")
      result.first.should == { "id" => 1, "name" => "John" }
    end

    it "should support bind values" do
      result = @adapter.query("SELECT * FROM users WHERE id = ?", 1)
      result.first["id"].should == 1
    end
  end # query

  describe "#create" do
    before(:all) do
      @adapter.execute("CREATE TABLE users (id integer primary key auto_increment, name varchar(255))")
    end

    it "should create a record" do
      @adapter.create([User.new(:name => "John")]).should == 1
    end
  end
end