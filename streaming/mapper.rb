#!/usr/bin/env ruby
require 'rbmongo_hadoop'

def mapper(documents)
  documents.each do |doc|
    yield { :_id => doc['user']['timezone'], :count => 1 }
  end
end

MongoHadoop::BSONMapper(mapper)
$stderr << "Done Mapping."