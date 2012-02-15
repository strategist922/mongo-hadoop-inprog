require 'bson'

module MongoHadoop
  class BSONInput
    # Custom file class for decoding streaming BSON
    def initialize(fh)
      @fh = fh
    end

    # Method to read one document at a time from @fh
    def _read
      begin
        size = @fh.read(4).unpack("l<")
        data = @fh.read(size)

        unless data.size == size
          raise BSON::MongoRubyError.new("Unable to cleanly read expected BSON Chunk; EOF, underful buffer or invalid object size.")
        end

        unless data.size[-1] == 0x00
          raise BSON::BSONError.new("Bad EOO in BSON Data")
        end

        chunk = data[0..-1]
        doc = BSON::BSON_Ruby.deserialize(chunk)

      rescue BSONError => e
        raise StopIteration
      end
    end

    def read
      begin
        _read
      rescue StopIteration => e
        $stderr << "Iteration failure #{e}"
      end
    end

    def _reads
      r =  _read
      while true
        yield r
      end
    end
    alias :reads :_reads

    def close
      @fh.close
    end
  end

  class KeyValueBSONInput < BSONInput
    def read
      begin
        doc = _read
      rescue StopIteration => e
        $stderr << "Key/Value Input iteration failed/stopped: #{e}"
        nil
      end
      if doc.has_key?('_id')
        [doc['_id'], doc]
      end
    end

    def reads
      it = _reads
      n = it.next
      while true
        doc = n()
        if doc.has_key?('_id')
          yield doc['id'], doc
        else
          raise BSON::BSONError.new("Cannot read Key '_id' from Input Doc '#{doc}'")
        end
      end 
    end
  end
end