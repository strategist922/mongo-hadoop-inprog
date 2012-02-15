module MongoHadoop
  class BSONMapper
    def initialize(target)
      output = MongoHadoop::BSONOutput.new
      input = MongoHadoop::BSONInput.new
    end
  end
end