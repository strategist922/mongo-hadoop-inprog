require 'test/unit'
require './input'
require './output'

BSON_FILE = File.expand_path(File.dirname(__FILE__) + "/test.bson")

TEST_DOC_TYLER = {
  "name"     => "Tyler Brock",
  "age"      => 27,
  "likes"    => ['computers', 'programming'],
  "location" => {
    "city"  => "New York",
    "state" => "NY",
    "zip"   => 10022
  }
}

TEST_DOC_FAKE = {
  "name" => "Fake Guy",
  "age"  => 56
}

TEST_DOCS = [ TEST_DOC_TYLER, TEST_DOC_FAKE ]

class TestInput < Test::Unit::TestCase
  
  def setup
    if File.exist?(BSON_FILE)
      File.delete(BSON_FILE)
    end
  end
  
  def teardown
  end
  
  def test_bson_round_trip
    fh = File.new(BSON_FILE, 'wb+')
    output = MongoHadoop::BSONOutput.new(fh)
    output.write(TEST_DOC_TYLER)
    fh.flush
    fh.close
    fh = File.new(BSON_FILE, 'rb+')
    input = MongoHadoop::BSONInput.new(fh)
    input_doc = input.read
    assert_equal TEST_DOC_TYLER, input_doc
  end

  def test_bson_write_many
    fh = File.new(BSON_FILE, 'wb+')
    output = MongoHadoop::BSONOutput.new(fh)
    output.write_all(TEST_DOCS)
    fh.flush
    fh.close
  end
  
  def test_bson_read_many
    fh = File.new(BSON_FILE, 'wb+')
    output = MongoHadoop::BSONOutput.new(fh)
    output.write_all(TEST_DOCS)
    fh.flush
    fh.close
    fh = File.new(BSON_FILE, 'rb+')
    input = MongoHadoop::BSONInput.new(fh)
    docs = input.collect {|doc| doc}
    assert_equal TEST_DOCS, docs
  end
end