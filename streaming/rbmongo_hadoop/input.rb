require 'bson'

module MongoHadoop
  # Custom file class for decoding streaming BSON
  class BSONInput
    include Enumerable
    
    def initialize(fh)
      @fh = fh
    end

    # Method to read one document at a time from @fh
    def read
      begin
        doc = BSON.read_bson_document(@fh)
      # No method error is thrown when no more docs
      # can be read from file handle -- stop iteration
      rescue NoMethodError
        raise StopIteration
      end
      return doc
    end
    
    # Needed to implement Enumerable
    def each
      loop do
        yield read
      end
    end
  end
  
  class KeyValueBSONInput < BSONInput
    def read
      doc = super
      
      if doc.has_key?('_id')
        return [doc['_id'], doc]
      else
        raise "Cannot read key '_id' from input document #{doc}"
      end
    end
  end
end