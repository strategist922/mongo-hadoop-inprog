require 'bson'

module MongoHadoop
  class BSONOutput
    def initialize(fh)
      @fh = fh
    end
    
    # Method to write one document at a time to @fh
    def write(obj)
      if obj.instance_of?(Hash)
        validate_write(obj)
        doc = BSON.serialize(obj)
        @fh.write(doc)
      else
        # Wrap non Hash object in Hash value field
        write({'value': obj})
      end
    end
    
    def write_all(enumerable)
      enumerable.each do |enum|
        write(enum)
      end
    end
    
    def validate_write(obj)
      if obj.has_key?('_id')
        raise "Output for BSON streaming must containg '_id' field, set to the key of the job"
    end
  end
  
  class KeyValueBSONOutput < BSONOutput
    def write(pair)
      if pair.instance_of?(Array)
        validate_write(pair)
        k = pair.first
        v = pair.last
        unless v.instance_of?(Hash)
          warn "Bare (or non Hash) output value of type #{v.class} found. Wrapping in a BSON object 'value' field."
          v = {'value': v}
        end
        v['_id'] = k
        super(v)
      else
        raise "Can only write a Array of (<key>, <value as a hash>). No support for direct BSON serialization of #{pair.class}"
      end
    end
    
    def validate_write(obj)
      if obj.instance_of?(Array)
        unless obj.size == 2
          raise "Key/Value output must contain 2 elements"
        if obj.last.instance_of?(Hash) and '_id' in obj.last
          warn "Warning: the Value contains an '_id', which will be overwritten by the contents of the key in KeyValueBSONOutput Mode"
        end
      end
    end
  end
end