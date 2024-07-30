/*
 *  File  : Join_op.hpp
 *
 *  Description: This file contains the header of the Join operator.
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

#ifndef JOIN_OP_HPP
#define JOIN_OP_HPP

#include <yahoo.hpp>
#include <templates/operators/util/joined_event.hpp>

#define EXEC_TIME 10

namespace spb{

/**
 * @brief This method implements the encapsulation of the Join operator.
 * 
 * @param item (&item) 
 */
void Join::op(spb::Item &item){
	Metrics metrics;
	volatile unsigned long latency_op;
	if(metrics.latency_is_enabled()){
		latency_op = current_time_usecs();
	}
	
	Join_op(item);

	if(metrics.latency_is_enabled()){
		item.latency_op.push_back(current_time_usecs() - latency_op);
	}
}

} // end of namespace spb

#endif /* JOIN_OP_HPP */
