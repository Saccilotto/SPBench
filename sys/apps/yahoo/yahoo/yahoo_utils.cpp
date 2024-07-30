/*
 *  File  : yahoo_utils.cpp
 *
 *  Description: This file contains the hidden functions of the yahoo application.
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

#include <yahoo_utils.hpp>
#define N_CAMPAIGNS 10
#define EXEC_TIME 10

namespace spb{
//Globals:
unsigned int value = 0; // value to be used as parameter for ads genration patterns
unsigned int event_type = 0; // the event type to be filtered

//std::vector<std::string> MemData; //vector to store data in-memory

// base campaign generator for comparison with the generated data
CampaignGenerator campaign_gen;
unordered_map<unsigned long, unsigned int> campaign_map = campaign_gen.getHashMap(); // hashmap
campaign_record *relational_table = campaign_gen.getRelationalTable(); // relational table
// arrays of ads per campaign generation
unsigned long **ads_arrays = campaign_gen.getArrays(); // arrays of ads
unsigned int adsPerCampaigns = campaign_gen.getAdsCampaign(); // number of ads per campaign

//var to store total generated ads as metric
unsigned int total_generated_ads = 0; 
long generated_tuples = 0;  

// time taken to execute the application
double time_taken; 
std::chrono::high_resolution_clock::time_point initial_time;

// Define a map to store campaign label as key and number of occurrences as value
#ifndef NO_DETAILS
std::map<int, int> campaign_events;
#endif


// bool stream_end = false;

void set_operators_name();
inline void usage(std::string);
void logEvent(unsigned long campaignId);

// void input_parser(char *);

void usage(std::string name){
	fprintf(stderr, "Usage: %s\n", name.c_str());
	fprintf(stderr, "  -i, --input            \"<some_string> <some_integer> ... \" (mandatory)\n");
	printGeneralUsage();
	exit(-1);
}

void init_bench(int argc, char* argv[]){
	int opt;
	int opt_index = 0;


	if(argc < 2) usage(argv[0]);

	try {
		//while ((opt = getopt(argc,argv,"i:t:b:B:m:M:F:u:p:klfxrh")) != EOF){
		while((opt = getopt_long(argc, argv, "i:t:b:B:m:M:f:F:Il:L:Tru:h", long_opts, &opt_index)) != -1) {
			switch(opt){
				case 'i':
					// if(split_string(optarg, ' ').size() < 2) 
					// 	throw std::invalid_argument("\n ARGUMENT ERROR --> Invalid input. Required: -i \"<dataset> <dataset_iteractions> <input_id (optional)>\"\n");
					// input_parser(optarg);
					break;
				case 't':
				case 'm':
				case 'M':
				case 'f':
				case 'F':
				case 'I':
				case 'l':
				case 'L':
				case 'T':
				case 'r':
				case 'u':
					// all the above empty cases fall into this option
					//SPBench::parseCMDLine(opt, optarg);
					break;
				case 'b':
				case 'B':
					throw std::invalid_argument("BATCH OPTION ERROR\n SPBench still does not provide native batching mechanisms with key-by data partitioning support, wich is a requirement of this application. \n You can still use batching in SPBench with the acceleration library of your choice.");
				case 'h':
					std::cout << std::endl; 
					usage(argv[0]);
					break;
				default: /* '?' */
					std::cout << std::endl; 
					usage(argv[0]);
					exit(EXIT_FAILURE);
			}
		}
	} catch(const std::invalid_argument& e) {
		std::cerr << "exception: " << e.what() << std::endl;
		printf(" You can use -h to see more options.\n");
		exit(1);
	} catch (const char* msg) {
		std::cerr << "exception: " << msg << std::endl;
		printf(" You can use -h to see more options.\n");
		exit(1);
	} catch(...) {
		std::cout << std::endl; 
		exit(1);
	}

	SPBench::bench_path = argv[0];
	

	set_operators_name();
	// Metrics::enable_latency();

	if(Metrics::monitoring_thread_is_enabled()){
		Metrics::start_monitoring();
	}
}

void set_operators_name(){
	SPBench::addOperatorName("Source     ");
	SPBench::addOperatorName("Filter     ");
	SPBench::addOperatorName("Join       ");
	SPBench::addOperatorName("Aggregate  ");
	SPBench::addOperatorName("Sink       ");
}

std::chrono::high_resolution_clock::time_point Source::source_item_timestamp;

#ifndef NO_DETAILS
void init_maps() {
    for (int label = 0; label < N_CAMPAIGNS; ++label) {
        campaign_events[label] = 0;
    }
}

void logEvent(unsigned long campaignId) {
    campaign_events[campaignId]++;
}
#endif

bool Source::op(Item &item){
#ifndef NO_DETAILS
	init_maps();
#endif

	// frequency control mechanism
	
	//SPBench::item_frequency_control(source_item_timestamp);

	// item.timestamp = source_item_timestamp = std::chrono::high_resolution_clock::now();
    // auto batch_elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(source_item_timestamp.time_since_epoch()).count();	
	
	// unsigned long latency_op;
	// if(Metrics::latency_is_enabled()){
	// 	latency_op = std::chrono::duration_cast<std::chrono::microseconds>(source_item_timestamp.time_since_epoch()).count();;
	// }
	auto initial_time =  std::chrono::high_resolution_clock::now();;
	while(time_taken <= EXEC_TIME) { //main source loop
		// // batching management routines
		// if(SPBench::getBatchInterval()){
		// 	// Check if the interval of this batch is higher than the batch interval defined by the user
		// 	if(((current_time_usecs() - batch_elapsed_time) / 1000.0) >= SPBench::getBatchInterval()) break;
		// } else {
		// 	// If no batch interval is set, than try to close it by size
		// 	if(item.batch_size >= SPBench::getBatchSize()) break;
		// }
		// // This couples with batching interval to close the batch by size if a size higher than one is defined
		// if(SPBench::getBatchSize() > 1){
		// 	if(item.batch_size >= SPBench::getBatchSize()) break;
		// }

		/* ****************************************** */
		/* ****** YOUR CODE GOES HERE (SOURCE) ****** */
		/* ⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄ */
		total_generated_ads += adsPerCampaigns;	

		item.event.user_id = 0;
		item.event.page_id = 0;
		item.event.ad_id = ads_arrays[(value % 100000) % (N_CAMPAIGNS * adsPerCampaigns)][1];
		item.event.ad_type = (value % 100000) % 5;
		item.event.event_type = (value % 100000) % 3;
		item.event.ip = 1;
		
		value++;
		item.setIndex(value);

		generated_tuples++;
	}

	//if this batch has size 0, ends computation
	if(item.batch_size == 0){
		return false;
	}


	item.batch_index = Metrics::batch_counter;
	Metrics::batch_counter++;	// sent batches
	return true;
}

void Sink::op(Item &item){
	
	// unsigned long latency_op;
	// if(Metrics::latency_is_enabled()){
	// 	latency_op = current_time_usecs();
	// }	

	auto possible_source_end = chrono::high_resolution_clock::now();
	while(time_taken <= EXEC_TIME || item.getItemData().getAggregateToSink().size() > 0){
        time_taken = chrono::duration_cast<chrono::seconds>(possible_source_end - initial_time).count();

		item.getItemData().getAggregateToSink().pop_back();
	}

	item.getItemData().~item_data();   // free memory


	// Metrics::batches_at_sink_counter++;

	// if(Metrics::latency_is_enabled()){
	// 	double current_time_sink = current_time_usecs();
	// 	item.latency_op.push_back(current_time_sink - latency_op);

	// 	unsigned long total_item_latency = (current_time_sink - item.timestamp);
	// 	Metrics::global_latency_acc += total_item_latency; // to compute real time average latency

	// 	auto latency = Metrics::Latency_t();
	// 	latency.local_latency = item.latency_op;
	// 	latency.total_latency = total_item_latency;
	// 	latency.item_timestamp = item.timestamp;
	// 	latency.item_sink_timestamp = current_time_sink;
	// 	latency.batch_size = item.batch_size;
	// 	Metrics::latency_vector.push_back(latency);
	// 	item.latency_op.clear();
	// }
	// if(Metrics::monitoring_is_enabled()){
	// 	Metrics::monitor_metrics();
	// }
}

void printToTerminal() {
#ifndef NO_DETAILS
	for (const auto &pair : campaign_events) {
		cout << "Campaign ID: " << pair.first << " - Number of Events: " << pair.second << endl;
	}
#endif
	cout << "Total number of generated tuples: " << generated_tuples << endl;
	cout << "Total generated ads: " << total_generated_ads << endl;
	cout << "Total time taken: " << time_taken << endl;
}

void printToOutput() {
#ifndef NO_OUTPUT
	ofstream outfile("output.txt");
	if (outfile.is_open()) {
#ifndef NO_DETAILS
	for (const auto &pair : campaign_events) {
		outfile << "Campaign ID: " << pair.first << " - Number of Events: " << pair.second << endl;
	}
#endif
		outfile << "Total number of generated tuples: " << generated_tuples << endl;
		outfile << "Total generated ads: " << total_generated_ads << endl;
		outfile << "Total time taken: " << time_taken << endl;
		outfile.close();
	} else {
		cerr << "Unable to open file for writing" << endl;
	}
#endif
}

void end_bench(){
	/* ******************************************* */
	/* ***** YOUR CODE GOES HERE (END BENCH)  **** */
	/* ⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄⌄ */
#ifndef NO_OUTPUT
	printToTerminal();
	printToOutput();
#elif defined(NO_OUTPUT)
	printToTerminal();
#endif


}

} //end of namespace spb