/*
 *  File  : yahoo.hpp
 *
 *  Description: This file contains the header of the yahoo application.
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

#ifndef YAHOO_HPP
#define YAHOO_HPP

#include <yahoo_utils.hpp>

namespace spb{
class Filter;
class Join;
class Aggregate;

class Filter{
private:
	static inline void Filter_op(Item &item);
public:
	static void op(Item &item);
	Filter(Item &item){
		op(item);
	}
	Filter(){};

	virtual ~Filter(){}
};

class Join{
private:
	static inline void Join_op(Item &item);
public:
	static void op(Item &item);
	Join(Item &item){
		op(item);
	}
	Join(){};

	virtual ~Join(){}
};

class Aggregate{
private:
	static inline void Aggregate_op(Item &item);
public:
	static void op(Item &item);
	Aggregate(Item &item){
		op(item);
	}
	Aggregate(){};

	virtual ~Aggregate(){}
};

} // end of namespace spb
#endif
