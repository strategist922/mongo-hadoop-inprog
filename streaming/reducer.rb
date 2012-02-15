#!/usr/bin/env ruby
require 'rbmongo_hadoop'

def reducer(key, values)
	$stderr << "Processing Key: #{key}"

	_count = 0
	values.each do |v|
		_count += v['count']
	
	{ :_id => key, :count => _count }
end

MongoHadoop::BSONReducer(reducer)
