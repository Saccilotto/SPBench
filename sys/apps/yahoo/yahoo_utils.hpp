/*
 *  File  : yahoo_utils.hpp
 *
 *  Description: This file contains the header for the hidden elements of the yahoo application.
 *
 *  Author: Adriano Marques Garcia
 *  Created on: July 22, 2024
 *
 *  License: GNU General Public License v3.0
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef YAHOO_UTILS_UTILS_HPP
#define YAHOO_UTILS_UTILS_HPP

/** Includes **/
#include <spbench.hpp>
#include <chrono>
#include <vector>
#include <unordered_map>
#include <map>

#include <templates/operators/util/campaign_generator.hpp>
#include <templates/operators/util/event.hpp>
#include <templates/operators/util/joined_event.hpp>
#include <templates/operators/util/result.hpp>

namespace spb{
#define NUMBER_OF_OPERATORS 5

struct item_data;
class Item;
class Source;
class Sink;

void init_bench(int argc, char* argv[]);
void end_bench();


struct item_data {
	vector<Item> source_to_filter;
	vector<Item> filter_to_join;
	vector<Item> join_to_aggregate;
	vector<Item> aggregate_to_sink;

    vector<Item>& getSourceToFilter() {
        return source_to_filter;
    }

    vector<Item>& getFilterToJoin() {
        return filter_to_join;
    }

    vector<Item>& getJoinToAggregate() {
        return join_to_aggregate;
    }

    vector<Item>& getAggregateToSink() {
        return aggregate_to_sink;
    }

	item_data():
		source_to_filter(),
        filter_to_join(),
        join_to_aggregate(),
        aggregate_to_sink()    
	{};

	~item_data(){
        source_to_filter.clear();
        filter_to_join.clear();
        join_to_aggregate.clear();
        aggregate_to_sink.clear();
	}
};

class Item : public Batch {
public:
    event_t event;
    joined_event_t joined_event;
    result_t result;
    item_data data; 
	unsigned int index;
    time_t timestamp;

    void setIndex(unsigned int _index) {
        index = _index;
    }

    Item(): 
        Batch(NUMBER_OF_OPERATORS), 
        event(),
		joined_event(),
		result(),
        data(),
        index(0)
    {};

    ~Item() {}

    item_data& getItemData() {
        return data;
    }
};  

class Source{
public:
	static std::chrono::high_resolution_clock::time_point source_item_timestamp;
	static bool op(Item &item);
	Source(){}
	virtual ~Source(){}
};

class Sink{
public:
	static void op(Item &item);
	Sink(){}
	virtual ~Sink(){}
};


/* Data from external input */

// struct workload_data {
// 	std::string some_input_file;
// 	int some_integer;
// 	std::string id;
// };
// extern workload_data input_data;


/* All the data communicated between operators */

// struct item_data {
// 	std::string *some_pointer;
// 	std::string some_string;
// 	unsigned int some_unsigned_integer;
// 	std::vector<std::string> some_vector;
// 	unsigned int index;

// 	item_data():
// 		some_pointer(NULL),
// 		some_unsigned_integer(0)
// 	{};

// 	~item_data(){
// 		some_vector.clear();
// 	}
// };

/* This class implements an Item */

// class Item : public Batch{
// public:
// 	std::vector<item_data> item_batch;

// 	Item():Batch(NUMBER_OF_OPERATORS){};

// 	~Item(){}
// };


} //end of namespace spb

#endif
