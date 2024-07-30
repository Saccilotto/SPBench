/*
 *  File  : yahoo.cpp
 *
 *  Description: This file contains the main function of the yahoo application.
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

#include <yahoo.hpp>

int main (int argc, char* argv[]){
	spb::init_bench(argc, argv); // Initializations
	spb::Metrics::init();
	while(1){
		spb::Item item;
		if(!spb::Source::op(item)) break;
		spb::Filter::op(item);
		spb::Join::op(item);
		spb::Aggregate::op(item);
		spb::Sink::op(item);
	}
	spb::Metrics::stop();
	spb::end_bench();
	return 0;
}
