require File.dirname(__FILE__) + "/helper"

include DataMapper::Adapters

describe "JdbcAdapter" do
  before(:all) do
    # DataMapper.logger = DataMapper::Logger.new($stdout, :debug)
    case ENV["ADAPTER"]
    when "mysql"
      `mysqladmin -uroot create jdbc_test`
      @adapter = DataMapper.setup(:default, "jdbc:mysql://127.0.0.1:3306/jdbc_test?user=root")
      @adapter.execute <<-EOS
      CREATE TABLE users
        (id integer primary key auto_increment, name varchar(255), age integer, weight float)
      EOS
    else
      @database = (Pathname(__FILE__).dirname.expand_path + "test.db").to_s
      @adapter = DataMapper.setup(:default, "jdbc:sqlite:#{@database}")
      @adapter.execute <<-EOS
      CREATE TABLE users
        (id integer primary key autoincrement, name varchar(255), age integer, weight float)
      EOS
    end
  end

  after(:all) do
    case ENV["ADAPTER"]
    when "mysql"
      `mysqladmin -f -uroot drop jdbc_test`
    else
      File.unlink(@database)
    end
  end

  before(:each) do
    @adapter.execute("DELETE FROM users")
  end

  it "should handle strings" do
    @adapter.execute("INSERT INTO users (name) VALUES (?)", "John")
    @adapter.query("SELECT name FROM users").first["name"].should == "John"
  end

  it "should handle integers" do
    @adapter.execute("INSERT INTO users (age) VALUES (?)", 23)
    @adapter.query("SELECT age FROM users").first["age"].should == 23
  end

  it "should handle floats" do
    pending
    @adapter.execute("INSERT INTO users (weight) VALUES (?)", 123.4)
    @adapter.query("SELECT weight FROM users").first["weight"].should == 123.4
  end

  it "should handle booleans" do
    pending
    [true, false].each do |boolean|
      @adapter.execute("INSERT INTO users (active) VALUES (?)", boolean)
      @adapter.query("SELECT active FROM users").first["active"].should == boolean
    end
  end

  it "should handle timestamps" do
    pending
    @adapter.execute("INSERT INTO users (created_at) VALUES (?)", Time.now)
    puts @adapter.query("SELECT created_at FROM users").inspect
  end

end