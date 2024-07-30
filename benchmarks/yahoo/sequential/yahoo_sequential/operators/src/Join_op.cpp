/*
 *  File  : Join_op.cpp
 *
 *  Description: This file contains the code of the Join operator.
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

#include <../include/Join_op.hpp>

namespace spb {
	extern double time_taken;
	extern unordered_map<unsigned long, unsigned int> campaign_map;
	extern std::vector<campaign_record> relational_table;
	extern void logEvent(unsigned long cmp_id);
}

/**
 * @brief This function implements the Join operator.
 * 
 * @param item (&item) 
 */
inline void spb::Join::Join_op(spb::Item &item){

	while(time_taken <= EXEC_TIME ||  item.getItemData().getFilterToJoin().size() > 0){
		Item it = item.getItemData().getFilterToJoin().back();
		item.getItemData().getFilterToJoin().pop_back();

		auto item_campaign = campaign_map.find(it.event.ad_id);
		if (item_campaign == campaign_map.end()) {
			continue;
		} else {
			campaign_record record = relational_table[(*item_campaign).second];
			it.joined_event = joined_event_t(record.cmp_id, 0);
			it.joined_event.ts = it.event.ts;
			it.joined_event.ad_id = it.event.ad_id;
			it.joined_event.relational_ad_id = record.ad_id;
			it.joined_event.cmp_id = record.cmp_id;
			#ifndef NO_DETAILS
			logEvent(item.joined_event.cmp_id);
			#endif
		}

		item.getItemData().getFilterToJoin().push_back(it);
	}
}
