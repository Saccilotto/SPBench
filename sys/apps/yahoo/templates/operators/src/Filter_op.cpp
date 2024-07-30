/*
 *  File  : Filter_op.cpp
 *
 *  Description: This file contains the code of the Filter operator.
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

#include <../include/Filter_op.hpp>

namespace spb {
    extern int event_type;
	extern double time_taken;
}

/**
 * @brief This function implements the Filter operator.
 * 
 * @param item (&item) 
 */
inline void spb::Filter::Filter_op(spb::Item &item){
	while(time_taken <= EXEC_TIME || item.getItemData().getSourceToFilter().size() > 0){
		Item it = item.getItemData().getSourceToFilter().back();
		item.getItemData().getSourceToFilter().pop_back();
		if (it.event.event_type != event_type) {
			return;
		}
		item.getItemData().getFilterToJoin().push_back(it);
	}
}
