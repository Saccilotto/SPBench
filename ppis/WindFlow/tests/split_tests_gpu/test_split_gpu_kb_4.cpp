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
 *  Test 4 split between MultiPipes with both CPU and GPU operators. Version
 *  with keyby distributions.
 *  
 *                                                  +----------------------+
 *                                                  |  +-----+    +-----+  |
 *                                                  |  |  M  |    |  S  |  |
 *                                         +------->+  | GPU +--->+ CPU |  |
 *                                         |        |  | (*) |    | (*) |  |
 *  +---------------------------------+    |        |  +-----+    +-----+  |
 *  |  +-----+    +-----+    +-----+  |    |        +----------------------+
 *  |  |  S  |    |  M  |    |  M  |  |    |
 *  |  | CPU +--->+ GPU +--->+ GPU |  +----+
 *  |  | (*) |    | (*) |    | (*) |  |    |
 *  |  +-----+    +-----+    +-----+  |    |
 *  +---------------------------------+    |
 *                                         |        +----------------------+
 *                                         |        |  +-----+    +-----+  |
 *                                         |        |  |  M  |    |  S  |  |
 *                                         +------->+  | CPU +--->+ CPU |  |
 *                                                  |  | (*) |    | (*) |  |
 *                                                  |  +-----+    +-----+  |
 *                                                  +----------------------+
 */ 

// include
#include<random>
#include<iostream>
#include<windflow.hpp>
#include<windflow_gpu.hpp>
#include"split_common_gpu_kb.hpp"

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
    // initalize global variable
    global_sum = 0;
    // arguments from command line
    if (argc != 5) {
        cout << argv[0] << " -r [runs] -l [stream_length]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "r:l:")) != -1) {
        switch (option) {
            case 'r': runs = atoi(optarg);
                     break;
            case 'l': stream_len = atoi(optarg);
                     break;
            default: {
                cout << argv[0] << " -r [runs] -l [stream_length]" << endl;
                exit(EXIT_SUCCESS);
            }
        }
    }
    // set random seed
    mt19937 rng;
    rng.seed(std::random_device()());
    size_t min = 1;
    size_t max = 4;
    std::uniform_int_distribution<std::mt19937::result_type> dist_p(min, max);
    std::uniform_int_distribution<std::mt19937::result_type> dist_b(100, 200);
    int map1_degree, map2_degree, map3_degree, map4_degree, sink1_degree, sink2_degree;
    size_t source_degree = dist_p(rng);
    long last_result = 0;
    // executes the runs in DEFAULT mode
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist_p(rng);
        map2_degree = dist_p(rng);
        map3_degree = dist_p(rng);
        map4_degree = dist_p(rng);
        sink1_degree = dist_p(rng);
        sink2_degree = dist_p(rng);
        cout << "Run " << i << endl;
        cout << "                                                +----------------------+" << endl;
        cout << "                                                |  +-----+    +-----+  |" << endl;
        cout << "                                                |  |  M  |    |  S  |  |" << endl;
        cout << "                                       +------->+  | GPU +--->+ CPU |  |" << endl;
        cout << "                                       |        |  | (" << map3_degree << ") |    | (" << sink1_degree << ") |  |" << endl;
        cout << "+---------------------------------+    |        |  +-----+    +-----+  |" << endl;
        cout << "|  +-----+    +-----+    +-----+  |    |        +----------------------+" << endl;
        cout << "|  |  S  |    |  M  |    |  M  |  |    |" << endl;
        cout << "|  | CPU +--->+ GPU +--->+ GPU |  +----+" << endl;
        cout << "|  | (" << source_degree << ") |    | (" << map1_degree << ") |    | (" << map2_degree << ") |  |    |" << endl;
        cout << "|  +-----+    +-----+    +-----+  |    |" << endl;
        cout << "+---------------------------------+    |" << endl;
        cout << "                                       |        +----------------------+" << endl;
        cout << "                                       |        |  +-----+    +-----+  |" << endl;
        cout << "                                       |        |  |  M  |    |  S  |  |" << endl;
        cout << "                                       +------->+  | CPU +--->+ CPU |  |" << endl;
        cout << "                                                |  | (" << map4_degree << ") |    | (" << sink2_degree << ") |  |" << endl;
        cout << "                                                |  +-----+    +-----+  |" << endl;
        cout << "                                                +----------------------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source_degree;
        if (source_degree != map1_degree) {
            check_degree += map1_degree;
        }
        check_degree += map2_degree;
        check_degree += map3_degree;
        if (map3_degree != sink1_degree) {
            check_degree += sink1_degree;
        }
        check_degree += map4_degree;
        if (map4_degree != sink2_degree) {
            check_degree += sink2_degree;
        }
        // prepare the test
        PipeGraph graph("test_split_gpu_kb_4", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
        // prepare the first MultiPipe
        Source_Functor source_functor1(stream_len, true);
        Source source1 = Source_Builder(source_functor1)
                            .withName("source1")
                            .withParallelism(source_degree)
                            .withOutputBatchSize(100)
                            .build();
        MultiPipe &pipe1 = graph.add_source(source1);
        Map_Functor_GPU map_functor_gpu1; 
        Map_GPU mapgpu1 = MapGPU_Builder(map_functor_gpu1)
                                .withName("mapgpu1")
                                .withParallelism(map1_degree)
                                .build();
        pipe1.chain(mapgpu1);
        Map_Functor_GPU_KB map_functor_gpu2; 
        Map_GPU mapgpu2 = MapGPU_Builder(map_functor_gpu2)
                                .withName("mapgpu2")
                                .withParallelism(map2_degree)
                                .withKeyBy([] __host__ __device__(const tuple_t &t) -> char { return t.key; })
                                .build();
        pipe1.chain(mapgpu2);
        // split
        pipe1.split_gpu<tuple_t>(2);
        // prepare the second MultiPipe
        MultiPipe &pipe2 = pipe1.select(0);
        Map_Functor_GPU_KB map_functor_gpu3; 
        Map_GPU mapgpu3 = MapGPU_Builder(map_functor_gpu3)
                                .withName("mapgpu3")
                                .withParallelism(map3_degree)
                                .withKeyBy([] __host__ __device__(const tuple_t &t) -> char { return t.key; })
                                .build();
        pipe2.chain(mapgpu3);
        Sink_Functor sink_functor1;
        Sink sink1 = Sink_Builder(sink_functor1)
                        .withName("sink1")
                        .withParallelism(sink1_degree)
                        .build();
        pipe2.chain_sink(sink1);
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.select(1); 
        Map_Functor map_functor4; 
        Map map4 = Map_Builder(map_functor4)
                        .withName("map4")
                        .withParallelism(map4_degree)
                        .withKeyBy([](const tuple_t &t) -> char { return t.key; })
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe3.chain(map4);
        Sink_Functor sink_functor2;
        Sink sink2 = Sink_Builder(sink_functor2)
                        .withName("sink2")
                        .withParallelism(sink2_degree)
                        .build();
        pipe3.chain_sink(sink2);
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
    return 0;
}
