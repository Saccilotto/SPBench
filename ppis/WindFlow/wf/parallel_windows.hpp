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

/** 
 *  @file    parallel_windows.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Parallel_Windows operator
 *  
 *  @section Parallel_Windows (Description)
 *  
 *  This file implements the Parallel_Windows operator able to execute incremental
 *  or non-incremental queries on count- or time-based windows. If created with
 *  parallelism greater than one, distinct windows are executed in parallel (both
 *  of different and of the same keyed substreams).
 */ 

#ifndef PAR_WIN_H
#define PAR_WIN_H

/// includes
#include<string>
#include<functional>
#include<context.hpp>
#include<single_t.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>
#include<basic_operator.hpp>
#include<window_replica.hpp>

namespace wf {

/** 
 *  \class Parallel_Windows
 *  
 *  \brief Parallel_Windows operator
 *  
 *  This class implements the Parallel_Windows operator executing incremental or
 *  non-incremental queries on streaming windows. The operator allows distinct
 *  windows to be processed in parallel (both of the same and of different
 *  keyed substreams).
 */ 
template<typename win_func_t, typename key_extractor_func_t>
class Parallel_Windows: public Basic_Operator
{
private:
    friend class MultiPipe; // friendship with the MultiPipe class
    friend class PipeGraph; // friendship with the PipeGraph class
    template<typename T1, typename T2, typename T3> friend class Paned_Windows; // friendship with the Paned_Windows class
    template<typename T1, typename T2, typename T3> friend class MapReduce_Windows; // friendship with the MapReduce_Windows class
    win_func_t func; // functional logic used by the Parallel_Windows
    key_extractor_func_t key_extr; // logic to extract the key attribute from the tuple_t
    size_t parallelism; // parallelism of the Parallel_Windows
    std::string type; // string describing the type of the Parallel_Windows
    std::string name; // name of the Parallel_Windows
    bool input_batching; // if true, the Parallel_Windows expects to receive batches instead of individual inputs
    size_t outputBatchSize; // batch size of the outputs produced by the Parallel_Windows
    std::vector<Window_Replica<win_func_t, key_extractor_func_t>*> replicas; // vector of pointers to the replicas of the Parallel_Windows
    uint64_t win_len; // window length (in no. of tuples or in time units)
    uint64_t slide_len; // slide length (in no. of tuples or in time units)
    uint64_t lateness; // triggering delay in time units (meaningful for TB windows in DEFAULT mode)
    Win_Type_t winType; // window type (CB or TB)

    // Configure the Parallel_Windows to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the Parallel_Windows
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Parallel_Windows has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the Parallel_Windows (i.e., the one of its PipeGraph)
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        for (auto *r: replicas) {
            r->setExecutionMode(_execution_mode);
        }
    }

    // Get the logic to extract the key attribute from the tuple_t
    key_extractor_func_t getKeyExtractor() const
    {
        return key_extr;
    }

#if defined (WF_TRACING_ENABLED)
    // Dump the log file (JSON format) of statistics of the Parallel_Windows
    void dumpStats() const override
    {
        std::ofstream logfile; // create and open the log file in the WF_LOG_DIR directory
#if defined (WF_LOG_DIR)
        std::string log_dir = std::string(STRINGIFY(WF_LOG_DIR));
        std::string filename = std::string(STRINGIFY(WF_LOG_DIR)) + "/" + std::to_string(getpid()) + "_" + name + ".json";
#else
        std::string log_dir = std::string("log");
        std::string filename = "log/" + std::to_string(getpid()) + "_" + name + ".json";
#endif
        if (mkdir(log_dir.c_str(), 0777) != 0) { // create the log directory
            struct stat st;
            if((stat(log_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        logfile.open(filename);
        rapidjson::StringBuffer buffer; // create the rapidjson writer
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        this->appendStats(writer); // append the statistics of the Map
        logfile << buffer.GetString();
        logfile.close();
    }

    // Append the statistics (JSON format) of the Parallel_Windows to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String(name.c_str());
        writer.Key("Operator_type");
        writer.String("Parallel_Windows");
        writer.Key("Distribution");
        writer.String("BROADCAST");
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isWindowed");
        writer.Bool(true);
        writer.Key("isGPU");
        writer.Bool(false);
        writer.Key("Window_type");
        if (winType == Win_Type_t::CB) {
            writer.String("count-based");
        }
        else {
            writer.String("time-based");
            writer.Key("Lateness");
            writer.Uint(lateness);  
        }
        writer.Key("Window_length");
        writer.Uint(win_len);
        writer.Key("Window_slide");
        writer.Uint(slide_len);
        writer.Key("Parallelism");
        writer.Uint(parallelism);        
        writer.Key("areNestedOPs"); // this for backward compatibility
        writer.Bool(false);
        writer.Key("OutputBatchSize");
        writer.Uint(outputBatchSize);
        writer.Key("Replicas");
        writer.StartArray();
        for (auto *r: replicas) { // append the statistics from all the replicas of the Keyed_Windows
            Stats_Record record = r->getStatsRecord();
            record.appendStats(writer);
        }
        writer.EndArray();
        writer.EndObject();
    }
#endif

    // Private Constructor
    Parallel_Windows(win_func_t _func,
                     key_extractor_func_t _key_extr,
                     size_t _parallelism,
                     std::string _name,
                     size_t _outputBatchSize,
                     std::function<void(RuntimeContext &)> _closing_func,
                     uint64_t _win_len,
                     uint64_t _slide_len,
                     uint64_t _lateness,
                     Win_Type_t _winType,
                     role_t _role):
                     func(_func),
                     key_extr(_key_extr),
                     parallelism(_parallelism),
                     type("Parallel_Windows"),
                     name(_name),
                     input_batching(false),
                     outputBatchSize(_outputBatchSize),
                     win_len(_win_len),
                     slide_len(_slide_len),
                     lateness(_lateness),
                     winType(_winType)
    {
        if (parallelism == 0) { // check the validity of the parallelism value
            std::cerr << RED << "WindFlow Error: Parallel_Windows has parallelism zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (win_len == 0 || slide_len == 0) { // check the validity of the windowing parameters
            std::cerr << RED << "WindFlow Error: Parallel_Windows used with window length or slide equal to zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        if (_role != role_t::MAP) {
            for (size_t i=0; i<parallelism; i++) { // create the internal replicas of the Parallel_Windows
                replicas.push_back(new Window_Replica<win_func_t, key_extractor_func_t>(func, 
                                                                                        key_extr,
                                                                                        name,
                                                                                        RuntimeContext(parallelism, i),
                                                                                        _closing_func,
                                                                                        win_len,
                                                                                        slide_len * parallelism,
                                                                                        lateness,
                                                                                        winType,
                                                                                        _role,
                                                                                        i,
                                                                                        parallelism));
            }
        }
        else {
            for (size_t i=0; i<parallelism; i++) { // create the internal replicas of the Parallel_Windows
                replicas.push_back(new Window_Replica<win_func_t, key_extractor_func_t>(func, 
                                                                                        key_extr,
                                                                                        name,
                                                                                        RuntimeContext(parallelism, i),
                                                                                        _closing_func,
                                                                                        win_len,
                                                                                        slide_len,
                                                                                        lateness,
                                                                                        winType,
                                                                                        _role,
                                                                                        0,
                                                                                        1));
            }           
        }
    }

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional logic of the Parallel_Windows (a function or a callable type)
     *  \param _key_extr key extractor (a function or a callable type)
     *  \param _parallelism internal parallelism of the Parallel_Windows
     *  \param _name name of the Parallel_Windows
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the Parallel_Windows (a function or callable type)
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _lateness (lateness in time units, meaningful for TB windows in DEFAULT mode)
     *  \param _winType window type (count-based CB or time-based TB)
     */ 
    Parallel_Windows(win_func_t _func,
                     key_extractor_func_t _key_extr,
                     size_t _parallelism,
                     std::string _name,
                     size_t _outputBatchSize,
                     std::function<void(RuntimeContext &)> _closing_func,
                     uint64_t _win_len,
                     uint64_t _slide_len,
                     uint64_t _lateness,
                     Win_Type_t _winType):
                     Parallel_Windows(_func, _key_extr, _parallelism, _name, _outputBatchSize, _closing_func, _win_len, _slide_len, _lateness, _winType, role_t::SEQ) {}

    /// Copy constructor
    Parallel_Windows(const Parallel_Windows &_other):
                     func(_other.func),
                     key_extr(_other.key_extr),
                     parallelism(_other.parallelism),
                     type(_other.type),
                     name(_other.name),
                     input_batching(_other.input_batching),
                     outputBatchSize(_other.outputBatchSize),
                     win_len(_other.win_len),
                     slide_len(_other.slide_len),
                     lateness(_other.lateness),
                     winType(_other.winType)
    {
        for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Keyed_Windows replicas
            replicas.push_back(new Window_Replica<win_func_t, key_extractor_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Parallel_Windows() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /// Copy Assignment Operator
    Parallel_Windows& operator=(const Parallel_Windows &_other)
    {
        if (this != &_other) {
            func = _other.func;
            key_extr = _other.key_extr;
            parallelism = _other.parallelism;
            type = _other.type;
            name = _other.name;
            input_batching = _other.input_batching;
            outputBatchSize = _other.outputBatchSize;
            win_len = _other.win_len;
            slide_len = _other.slide_len;
            lateness = _other.lateness;
            winType = _other.winType;
            for (auto *r: replicas) { // delete all the replicas
                delete r;
            }
            replicas.clear();
            for (size_t i=0; i<parallelism; i++) { // deep copy of the pointers to the Parallel_Windows replicas
                replicas.push_back(new Window_Replica<win_func_t, key_extractor_func_t>(*(_other.replicas[i])));
            }
        }
        return *this;
    }

    /// Move Assignment Operator
    Parallel_Windows& operator=(Parallel_Windows &&_other)
    {
        func = std::move(_other.func);
        key_extr = std::move(_other.key_extr);
        parallelism = _other.parallelism;
        type = std::move(_other.type);
        name = std::move(_other.name);
        input_batching = _other.input_batching;
        outputBatchSize = _other.outputBatchSize;
        win_len = _other.win_len;
        slide_len = _other.slide_len;
        lateness = _other.lateness;
        winType = _other.winType;
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
        replicas = std::move(_other.replicas);
        return *this;
    }

    /** 
     *  \brief Get the type of the Parallel_Windows as a string
     *  \return type of the Parallel_Windows
     */ 
    std::string getType() const override
    {
        return type;
    }

    /** 
     *  \brief Get the name of the Parallel_Windows as a string
     *  \return name of the Parallel_Windows
     */ 
    std::string getName() const override
    {
        return name;
    }

    /** 
     *  \brief Get the total parallelism of the Parallel_Windows
     *  \return total parallelism of the Parallel_Windows
     */  
    size_t getParallelism() const override
    {
        return parallelism;
    }

    /** 
     *  \brief Return the input routing mode of the Parallel_Windows
     *  \return routing mode used to send inputs to the Parallel_Windows
     */ 
    Routing_Mode_t getInputRoutingMode() const override
    {
        return Routing_Mode_t::BROADCAST;
    }

    /** 
     *  \brief Return the size of the output batches that the Parallel_Windows should produce
     *  \return output batch size in number of tuples
     */ 
    size_t getOutputBatchSize() const override
    {
        return outputBatchSize;
    }

    /** 
     *  \brief Get the window type (CB or TB) utilized by the Parallel_Windows
     *  \return adopted windowing semantics (count-based or time-based)
     */ 
    Win_Type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of ignored tuples by the Parallel_Windows
     *  \return number of tuples ignored during the processing by the Parallel_Windows
     */ 
    size_t getNumIgnoredTuples() const
    {
        size_t count = 0;
        for (auto *r: replicas) {
            count += r->getNumIgnoredTuples();
        }
        return count;
    }
};

} // namespace wf

#endif
