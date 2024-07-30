/*
 *  File  : Aggregate_op.cpp
 *
 *  Description: This file contains the code of the Aggregate operator.
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

#include <../include/Aggregate_op.hpp>

namespace spb {
	extern double time_taken;
}

/**
 * 
 * Aggreagte function that increments the count and updates the lastUpdate field.
 * 
 */
void aggregateFunctionINC(const joined_event_t &event, result_t &result) {
    result.count++;
    if (event.ts > result.lastUpdate) {
        result.lastUpdate = event.ts;
    }
}

/**
 * @brief This function implements the Aggregate operator.
 * 
 * @param item (&item) 
 */
inline void spb::Aggregate::Aggregate_op(spb::Item &item){

	while(time_taken <= EXEC_TIME ||  item.getItemData().getJoinToAggregate().size() > 0){
		Item it = item.getItemData().getJoinToAggregate().back();
		item.getItemData().getFilterToJoin().pop_back();

		aggregateFunctionINC(it.joined_event, it.result);

		item.getItemData().getAggregateToSink().push_back(it);
	}
}
