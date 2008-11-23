require File.dirname(__FILE__) + "/helper"

include DataMapper::Adapters

describe "JdbcAdapter" do
  before(:all) do
    # DataMapper.logger = DataMapper::Logger.new($stdout, :debug)
    case ENV["ADAPTER"]
    when "mysql"
      `mysqladmin -uroot create jdbc_test`
      @adapter = DataMapper.setup(:default, "jdbc:mysql://127.0.0.1:3306/jdbc_test?user=root")
      @schema = <<-EOS
      CREATE TABLE users
        (id integer primary key auto_increment, name varchar(255), age integer, weight float)
      EOS
    else
      @database = (Pathname(__FILE__).dirname.expand_path + "test.db").to_s
      @adapter = DataMapper.setup(:default, "jdbc:sqlite:#{@database}")
      @schema = <<-EOS
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

  describe "#execute" do
    it "should execute a query" do
      @adapter.execute(@schema)
    end

    it "should accept bind values" do
      @adapter.execute("INSERT INTO users (name) VALUES (?)", "John")
    end

    it "should return generated keys when present" do
      key = @adapter.execute("INSERT INTO users (name) VALUES (?)", "John")
      key.should == 2
    end
  end # execute

  describe "#query" do
    before(:all) do
      @adapter.execute(@schema)
      @adapter.execute("INSERT INTO users (name) VALUES ('John')")
    end

    it "should return a result set" do
      result = @adapter.query("SELECT * FROM users").first
      result["id"].should == 1
      result["name"].should == "John"
      result["age"].should == nil
    end

    it "should support bind values" do
      result = @adapter.query("SELECT * FROM users WHERE id = ?", 1)
      result.first["id"].should == 1
    end
  end # query

  describe "#quote_identifier" do
    before(:all) do
      @adapter.class.send(:public, :quote_string, :quote_identifier)
      @quote = @adapter.quote_string
    end

    it "should quote a table name" do
      @adapter.quote_identifier("users").should == "#{@quote}users#{@quote}"
    end

    it "should quote a column name with tablename" do
      @adapter.quote_identifier("users.name").should == "#{@quote}users#{@quote}.#{@quote}name#{@quote}"
    end
  end

  describe "#create" do
    before(:all) do
      @adapter.execute(@schema)
    end

    it "should create a record" do
      @adapter.create([User.new(:name => "John")]).should == 1
    end
  end

  describe "#read_one" do
    before(:all) do
      @adapter.execute(@schema)
      @adapter.create([User.new(:name => "John")]).should == 1
    end

    it "should return with an empty query" do
      User.first.name.should == "John"
    end

    it "should return with a more complicated query" do
      User.first(:name.not => "James", :id.lt => 450).name.should == "John"
    end
  end

  describe "#update" do
    before(:all) do
      @adapter.execute(@schema)
    end

    it "should update a record" do
      pending
      user = User.new(:name => "John")
      user.save
      user.name = "James"
      user.save
      @adapter.query("SELECT name FROM users WHERE id=?", user.id).first["name"].should == "James"
    end
  end
end