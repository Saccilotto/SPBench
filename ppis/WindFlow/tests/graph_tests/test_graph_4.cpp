/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/vers3.x/LICENSE.MIT
 *  
 *  WindFlow is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

/*  
 *  Test 4 of general graphs of operators.
 *                                                                 +---------------------+
 *                                                                 |  +-----+   +-----+  |
 *                                                                 |  |  S  |   | FM  |  |
 *                                                                 |  | (*) +-->+ (*) |  +-+
 *                                                                 |  +-----+   +-----+  | |
 *                                                                 +---------------------+ |
 *                                                         +-----------+                   |
 *                                                         |  +-----+  |                   |
 *                                                      +->+  |  M  |  +-+                 |
 *                                                      |  |  | (*) |  | |                 |
 *                              +---------------------+ |  |  +-----+  | |   +-----------+ |  +-----------+
 *                              |  +-----+   +-----+  | |  +-----------+ |   |  +-----+  | |  |  +-----+  |
 *                           +->+  |  F  |   |  M  |  +-+                +-->+  |  M  |  | |  |  |  S  |  |
 *                           |  |  | (*) +-->+ (*) |  | |  +-----------+ |   |  | (*) |  +-+->+  | (*) |  |
 *                           |  |  +-----+   +-----+  | |  |  +-----+  | |   |  +-----+  | |  |  +-----+  |
 *  +---------------------+  |  +---------------------+ |  |  |  F  |  | |   +-----------+ |  +-----------+
 *  |  +-----+   +-----+  |  |                          +->+  | (*) |  +-+                 |
 *  |  |  S  |   |  M  |  |  |                             |  +-----+  |                   |
 *  |  | (*) +-->+ (*) |  +--+                             +-----------+                   |
 *  |  +-----+   +-----+  |  |                                                             |
 *  +---------------------+  |       +-----------+                                         |
 *                           |       |  +-----+  |                                         |
 *                           |       |  |  F  |  |                                         |
 *                           +------>+  | (*) |  +-----------------------------------------+
 *                                   |  +-----+  |
 *                                   +-----------+
 */ 

// include
#include<random>
#include<iostream>
#include<windflow.hpp>
#include"graph_common.hpp"

using namespace std;
using namespace wf;

// global variable for the result
extern atomic<long> global_sum;

// main
int main(int argc, char *argv[])
{
    int option = 0;
    size_t runs = 1;
    size_t stream_len = 0;
    size_t n_keys = 1;
    // initalize global variable
    global_sum = 0;
    // arguments from command line
    if (argc != 7) {
        cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "r:l:k:")) != -1) {
        switch (option) {
            case 'r': runs = atoi(optarg);
                     break;
            case 'l': stream_len = atoi(optarg);
                     break;
            case 'k': n_keys = atoi(optarg);
                     break;
            default: {
                cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys]" << endl;
                exit(EXIT_SUCCESS);
            }
        }
    }
    // set random seed
    mt19937 rng;
    rng.seed(std::random_device()());
    size_t min = 1;
    size_t max = 9;
    std::uniform_int_distribution<std::mt19937::result_type> dist_p(min, max);
    std::uniform_int_distribution<std::mt19937::result_type> dist_b(0, 10);
    int map1_degree, map2_degree, map3_degree, map4_degree, flatmap1_degree, filter1_degree, filter2_degree, filter3_degree, sink1_degree;
    size_t source1_degree = dist_p(rng);
    size_t source2_degree = dist_p(rng);
    long last_result = 0;
    // executes the runs in DEFAULT mode
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist_p(rng);
        map2_degree = dist_p(rng);
        map3_degree = dist_p(rng);
        map4_degree = dist_p(rng);
        flatmap1_degree = dist_p(rng);
        filter1_degree = dist_p(rng);
        filter2_degree = dist_p(rng);
        filter3_degree = dist_p(rng);
        sink1_degree = dist_p(rng);
        cout << "Run " << i << endl;
        cout << "                                                               +---------------------+" << endl;
        cout << "                                                               |  +-----+   +-----+  |" << endl;
        cout << "                                                               |  |  S  |   | FM  |  |" << endl;
        cout << "                                                               |  | (" << source2_degree << ") +-->+ (" << flatmap1_degree << ") |  +-+" << endl;
        cout << "                                                               |  +-----+   +-----+  | |" << endl;
        cout << "                                                               +---------------------+ |" << endl;
        cout << "                                                                                       |" << endl;
        cout << "                                                       +-----------+                   |" << endl;
        cout << "                                                       |  +-----+  |                   |" << endl;
        cout << "                                                    +->+  |  M  |  +-+                 |" << endl;
        cout << "                                                    |  |  | (" << map3_degree << ") |  | |                 |" << endl;
        cout << "                            +---------------------+ |  |  +-----+  | |   +-----------+ |  +-----------+" << endl;
        cout << "                            |  +-----+   +-----+  | |  +-----------+ |   |  +-----+  | |  |  +-----+  |" << endl;
        cout << "                         +->+  |  F  |   |  M  |  +-+                +-->+  |  M  |  | |  |  |  S  |  |" << endl;
        cout << "                         |  |  | (" << filter1_degree << ") +-->+ (" << map2_degree << ") |  | |  +-----------+ |   |  | (" << map4_degree << ") |  +-+->+  | ("<< sink1_degree << ") |  |" << endl;
        cout << "                         |  |  +-----+   +-----+  | |  |  +-----+  | |   |  +-----+  | |  |  +-----+  |" << endl;
        cout << "+---------------------+  |  +---------------------+ |  |  |  F  |  | |   +-----------+ |  +-----------+" << endl;
        cout << "|  +-----+   +-----+  |  |                          +->+  | (" << filter2_degree << ") |  +-+                 |" << endl;
        cout << "|  |  S  |   |  M  |  |  |                             |  +-----+  |                   |" << endl;
        cout << "|  | (" << source1_degree << ") +-->+ (" << map1_degree << ") |  +--+                             +-----------+                   |" << endl;
        cout << "|  +-----+   +-----+  |  |                                                             |" << endl;
        cout << "+---------------------+  |       +-----------+                                         |" << endl;
        cout << "                         |       |  +-----+  |                                         |" << endl;
        cout << "                         |       |  |  F  |  |                                         |" << endl;
        cout << "                         +------>+  | (" << filter3_degree << ") |  +-----------------------------------------+" << endl;
        cout << "                                 |  +-----+  |" << endl;
        cout << "                                 +-----------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source1_degree;
        if (source1_degree != map1_degree) {
            check_degree += map1_degree;
        }
        check_degree += filter1_degree;
        if (filter1_degree != map2_degree) {
            check_degree += map2_degree;
        }
        check_degree += map3_degree;
        check_degree += filter2_degree;
        check_degree += map4_degree;
        check_degree += filter3_degree;
        check_degree += source2_degree;
        if (source2_degree != flatmap1_degree) {
            check_degree += flatmap1_degree;
        }
        check_degree += sink1_degree;
        // prepare the test
        PipeGraph graph("test_graph_4 (DEFAULT)", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
        // prepare the first MultiPipe
        Source_Positive_Functor source_functor_positive(stream_len, n_keys, true);
        Source source1 = Source_Builder(source_functor_positive)
                            .withName("source1")
                            .withParallelism(source1_degree)
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        MultiPipe &pipe1 = graph.add_source(source1);
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                        .withName("map1")
                        .withParallelism(map1_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe1.chain(map1);        
        // split
        pipe1.split([](const tuple_t &t) {
            if (t.value % 2 == 0) {
                return 0;
            }
            else {
                return 1;
            }
        }, 2);
        // prepare the second MultiPipe
        MultiPipe &pipe2 = pipe1.select(0);
        Filter_Functor_KB filter_functor1(4);
        Filter filter1 = Filter_Builder(filter_functor1)
                                .withName("filter1")
                                .withParallelism(filter1_degree)
                                .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                .withOutputBatchSize(dist_b(rng))
                                .build();
        pipe2.chain(filter1);
        Map_Functor map_functor2;
        Map map2 = Map_Builder(map_functor2)
                        .withName("map2")
                        .withParallelism(map2_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe2.chain(map2);
        // split
        pipe2.split([](const tuple_t &t) {
            if (t.value % 3 == 0) {
                return 0;
            }
            else {
                return 1;
            }
        }, 2);        
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe2.select(0);
        Map_Functor map_functor3;
        Map map3 = Map_Builder(map_functor3)
                        .withName("map3")
                        .withParallelism(map3_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe3.chain(map3);
        // prepare the fourth MultiPipe
        MultiPipe &pipe4 = pipe2.select(1);
        Filter_Functor filter_functor2(5);
        Filter filter2 = Filter_Builder(filter_functor2)
                                .withName("filter2")
                                .withParallelism(filter2_degree)
                                .withOutputBatchSize(dist_b(rng))
                                .build();
        pipe4.chain(filter2);
        // prepare the fifth MultiPipe
        MultiPipe &pipe5 = pipe3.merge(pipe4);
        Map_Functor map_functor4;
        Map map4 = Map_Builder(map_functor4)
                        .withName("map4")
                        .withParallelism(map4_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe5.chain(map4);
        // prepare the sixth MultiPipe
        MultiPipe &pipe6 = pipe1.select(1);
        Filter_Functor_KB filter_functor3(7);
        Filter filter3 = Filter_Builder(filter_functor3)
                                .withName("filter3")
                                .withParallelism(filter3_degree)
                                .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                .withOutputBatchSize(dist_b(rng))
                                .build();
        pipe6.chain(filter3);
        // prepare the seventh MultiPipe
        Source_Negative_Functor source_functor_negative(stream_len, n_keys, true);
        Source source2 = Source_Builder(source_functor_negative)
                            .withName("source2")
                            .withParallelism(source2_degree)
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        MultiPipe &pipe7 = graph.add_source(source2);   
        FlatMap_Functor flatmap_functor1;
        FlatMap flatmap1 = FlatMap_Builder(flatmap_functor1)
                        .withName("flatmap1")
                        .withParallelism(flatmap1_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe7.chain(flatmap1);
        // prepare the eighth MultiPipe
        MultiPipe &pipe8 = pipe5.merge(pipe7, pipe6);
        Sink_Functor sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                        .withName("sink")
                        .withParallelism(sink1_degree)
                        .build();
        pipe8.chain_sink(sink);
        assert(graph.getNumThreads() == check_degree);
        // run the application
        graph.run();
        if (i == 0) {
            last_result = global_sum;
            cout << "Result is --> " << GREEN << "OK" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
        }
        else {
            if (last_result == global_sum) {
                cout << "Result is --> " << GREEN << "OK" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
            }
            else {
                cout << "Result is --> " << RED << "FAILED" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
                abort();
            }
        }
        global_sum = 0;
    }
    // executes the runs in DETERMINISTIC mode
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist_p(rng);
        map2_degree = dist_p(rng);
        map3_degree = dist_p(rng);
        map4_degree = dist_p(rng);
        flatmap1_degree = dist_p(rng);
        filter1_degree = dist_p(rng);
        filter2_degree = dist_p(rng);
        filter3_degree = dist_p(rng);
        sink1_degree = dist_p(rng);
        cout << "Run " << i << endl;
        cout << "                                                               +---------------------+" << endl;
        cout << "                                                               |  +-----+   +-----+  |" << endl;
        cout << "                                                               |  |  S  |   | FM  |  |" << endl;
        cout << "                                                               |  | (" << source2_degree << ") +-->+ (" << flatmap1_degree << ") |  +-+" << endl;
        cout << "                                                               |  +-----+   +-----+  | |" << endl;
        cout << "                                                               +---------------------+ |" << endl;
        cout << "                                                                                       |" << endl;
        cout << "                                                       +-----------+                   |" << endl;
        cout << "                                                       |  +-----+  |                   |" << endl;
        cout << "                                                    +->+  |  M  |  +-+                 |" << endl;
        cout << "                                                    |  |  | (" << map3_degree << ") |  | |                 |" << endl;
        cout << "                            +---------------------+ |  |  +-----+  | |   +-----------+ |  +-----------+" << endl;
        cout << "                            |  +-----+   +-----+  | |  +-----------+ |   |  +-----+  | |  |  +-----+  |" << endl;
        cout << "                         +->+  |  F  |   |  M  |  +-+                +-->+  |  M  |  | |  |  |  S  |  |" << endl;
        cout << "                         |  |  | (" << filter1_degree << ") +-->+ (" << map2_degree << ") |  | |  +-----------+ |   |  | (" << map4_degree << ") |  +-+->+  | ("<< sink1_degree << ") |  |" << endl;
        cout << "                         |  |  +-----+   +-----+  | |  |  +-----+  | |   |  +-----+  | |  |  +-----+  |" << endl;
        cout << "+---------------------+  |  +---------------------+ |  |  |  F  |  | |   +-----------+ |  +-----------+" << endl;
        cout << "|  +-----+   +-----+  |  |                          +->+  | (" << filter2_degree << ") |  +-+                 |" << endl;
        cout << "|  |  S  |   |  M  |  |  |                             |  +-----+  |                   |" << endl;
        cout << "|  | (" << source1_degree << ") +-->+ (" << map1_degree << ") |  +--+                             +-----------+                   |" << endl;
        cout << "|  +-----+   +-----+  |  |                                                             |" << endl;
        cout << "+---------------------+  |       +-----------+                                         |" << endl;
        cout << "                         |       |  +-----+  |                                         |" << endl;
        cout << "                         |       |  |  F  |  |                                         |" << endl;
        cout << "                         +------>+  | (" << filter3_degree << ") |  +-----------------------------------------+" << endl;
        cout << "                                 |  +-----+  |" << endl;
        cout << "                                 +-----------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source1_degree;
        if (source1_degree != map1_degree) {
            check_degree += map1_degree;
        }
        check_degree += filter1_degree;
        if (filter1_degree != map2_degree) {
            check_degree += map2_degree;
        }
        check_degree += map3_degree;
        check_degree += filter2_degree;
        check_degree += map4_degree;
        check_degree += filter3_degree;
        check_degree += source2_degree;
        if (source2_degree != flatmap1_degree) {
            check_degree += flatmap1_degree;
        }
        check_degree += sink1_degree;
        // prepare the test
        PipeGraph graph("test_graph_4 (DETERMINISTIC)", Execution_Mode_t::DETERMINISTIC, Time_Policy_t::EVENT_TIME);
        // prepare the first MultiPipe
        Source_Positive_Functor source_functor_positive(stream_len, n_keys, false);
        Source source1 = Source_Builder(source_functor_positive)
                            .withName("source1")
                            .withParallelism(source1_degree)
                            .build();
        MultiPipe &pipe1 = graph.add_source(source1);
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                        .withName("map1")
                        .withParallelism(map1_degree)
                        .build();
        pipe1.chain(map1);        
        // split
        pipe1.split([](const tuple_t &t) {
            if (t.value % 2 == 0) {
                return 0;
            }
            else {
                return 1;
            }
        }, 2);
        // prepare the second MultiPipe
        MultiPipe &pipe2 = pipe1.select(0);
        Filter_Functor_KB filter_functor1(4);
        Filter filter1 = Filter_Builder(filter_functor1)
                                .withName("filter1")
                                .withParallelism(filter1_degree)
                                .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                .build();
        pipe2.chain(filter1);
        Map_Functor map_functor2;
        Map map2 = Map_Builder(map_functor2)
                        .withName("map2")
                        .withParallelism(map2_degree)
                        .build();
        pipe2.chain(map2);
        // split
        pipe2.split([](const tuple_t &t) {
            if (t.value % 3 == 0) {
                return 0;
            }
            else {
                return 1;
            }
        }, 2);       
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe2.select(0);
        Map_Functor map_functor3;
        Map map3 = Map_Builder(map_functor3)
                        .withName("map3")
                        .withParallelism(map3_degree)
                        .build();
        pipe3.chain(map3);
        // prepare the fourth MultiPipe
        MultiPipe &pipe4 = pipe2.select(1);
        Filter_Functor filter_functor2(5);
        Filter filter2 = Filter_Builder(filter_functor2)
                                .withName("filter2")
                                .withParallelism(filter2_degree)
                                .build();
        pipe4.chain(filter2);
        // prepare the fifth MultiPipe
        MultiPipe &pipe5 = pipe3.merge(pipe4);
        Map_Functor map_functor4;
        Map map4 = Map_Builder(map_functor4)
                        .withName("map4")
                        .withParallelism(map4_degree)
                        .build();
        pipe5.chain(map4);
        // prepare the sixth MultiPipe
        MultiPipe &pipe6 = pipe1.select(1);
        Filter_Functor_KB filter_functor3(7);
        Filter filter3 = Filter_Builder(filter_functor3)
                                .withName("filter3")
                                .withParallelism(filter3_degree)
                                .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                .build();
        pipe6.chain(filter3);
        // prepare the seventh MultiPipe
        Source_Negative_Functor source_functor_negative(stream_len, n_keys, false);
        Source source2 = Source_Builder(source_functor_negative)
                            .withName("source2")
                            .withParallelism(source2_degree)
                            .build();
        MultiPipe &pipe7 = graph.add_source(source2);   
        FlatMap_Functor flatmap_functor1;
        FlatMap flatmap1 = FlatMap_Builder(flatmap_functor1)
                        .withName("flatmap1")
                        .withParallelism(flatmap1_degree)
                        .build();
        pipe7.chain(flatmap1);
        // prepare the eighth MultiPipe
        MultiPipe &pipe8 = pipe5.merge(pipe7, pipe6);
        Sink_Functor sink_functor;
        Sink sink = Sink_Builder(sink_functor)
                        .withName("sink")
                        .withParallelism(sink1_degree)
                        .build();
        pipe8.chain_sink(sink);
        assert(graph.getNumThreads() == check_degree);
        // run the application
        graph.run();
        if (last_result == global_sum) {
                cout << "Result is --> " << GREEN << "OK" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
        }
        else {
            cout << "Result is --> " << RED << "FAILED" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
            abort();
        }
        global_sum = 0;
    }
    return 0;
}
