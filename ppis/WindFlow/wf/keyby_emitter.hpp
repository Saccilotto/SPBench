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
 *  @file    keyby_emitter.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Emitter implementing the keyby (KB) distribution
 *  
 *  @section KeyBy_Emitter (Description)
 *  
 *  The emitter delivers each received tuple to one of its destinations by respecting the
 *  keyby distribution semantics. The emitter can be configured to work without batching
 *  (using Single_t structures) or in batched mode (using Batch_CPU_t structures).
 */ 

#ifndef KB_EMITTER_H
#define KB_EMITTER_H

// includes
#include<basic.hpp>
#include<single_t.hpp>
#include<batch_cpu_t.hpp>
#include<basic_emitter.hpp>

namespace wf {

// class KeyBy_Emitter
template<typename key_extractor_func_t>
class KeyBy_Emitter: public Basic_Emitter
{
private:
    key_extractor_func_t key_extr; // functional logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_KeyExtr(key_extr)); // extracting the tuple_t type and checking the admissible signatures
    size_t num_dests; // number of destinations connected in output to the emitter
    size_t size; // if >0 the emitter works in batched more, otherwise in a per-tuple basis
    bool useTreeMode; // true if the emitter is used in tree-based mode
    std::vector<std::pair<void *, size_t>> output_queue; // vector of pairs (messages and destination identifiers)
    std::vector<Batch_CPU_t<tuple_t> *> batches_output; // vector of pointers to the output batches one per destination (meaningful if size > 0)
    ff::MPMC_Ptr_Queue *queue; // pointer to the recyling queue
    Execution_Mode_t execution_mode; // execution mode of the emitter
    uint64_t last_time_punct; // last time used to send punctuations
    std::vector<int> delivered; // delivered[i] is the number of outputs delivered to destination i during the last sample
    uint64_t received_inputs; // total number of inputs received by the emitter
    std::vector<uint64_t> last_sent_wms; // vector of the last sent watermarks, one per destination;

public:
    // Constructor
    KeyBy_Emitter(key_extractor_func_t _key_extr,
                  size_t _num_dests,
                  Execution_Mode_t _execution_mode,
                  size_t _size=0):
                  key_extr(_key_extr),
                  num_dests(_num_dests),
                  size(_size),
                  useTreeMode(false),
                  batches_output(_num_dests, nullptr),
                  execution_mode(_execution_mode),
                  last_time_punct(current_time_usecs()),
                  delivered(_num_dests, 0),
                  received_inputs(0),
                  last_sent_wms(_num_dests, 0)
    {
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
    }

    // Copy Constructor
    KeyBy_Emitter(const KeyBy_Emitter &_other):
                  key_extr(_other.key_extr),
                  num_dests(_other.num_dests),
                  size(_other.size),
                  useTreeMode(_other.useTreeMode),
                  batches_output(_other.num_dests, nullptr),
                  execution_mode(_other.execution_mode),
                  last_time_punct(_other.last_time_punct),
                  delivered(_other.delivered),
                  received_inputs(_other.received_inputs),
                  last_sent_wms(_other.last_sent_wms)
    {
        queue = new ff::MPMC_Ptr_Queue();
        queue->init(DEFAULT_BUFFER_CAPACITY);
    }

    // Move Constructor
    KeyBy_Emitter(KeyBy_Emitter &&_other):
                  key_extr(std::move(_other.key_extr)),
                  num_dests(_other.num_dests),
                  size(_other.size),
                  useTreeMode(_other.useTreeMode),
                  output_queue(std::move(_other.output_queue)),
                  batches_output(std::move(_other.batches_output)),
                  queue(std::exchange(_other.queue, nullptr)),
                  execution_mode(_other.execution_mode),
                  last_time_punct(_other.last_time_punct),
                  delivered(std::move(_other.delivered)),
                  received_inputs(_other.received_inputs),
                  last_sent_wms(std::move(_other.last_sent_wms)) {}

    // Destructor
    ~KeyBy_Emitter() override
    {
        assert(output_queue.size() == 0); // sanity check
        for (auto *b: batches_output) {
            assert(b == nullptr); // sanity check
        }
        if (size == 0) { // delete all the Single_t items in the recycling queue
            if (queue != nullptr) {
                Single_t<decltype(get_tuple_t_KeyExtr(key_extr))> *del_single = nullptr;
                while (queue->pop((void **) &del_single)) {
                    delete del_single;
                }
                delete queue; // delete the recycling queue
            }
        }
        else { // delete all the batches in the recycling queue
            if (queue != nullptr) {
                Batch_t<decltype(get_tuple_t_KeyExtr(key_extr))> *del_batch = nullptr;
                while (queue->pop((void **) &del_batch)) {
                    delete del_batch;
                }
                delete queue; // delete the recycling queue
            }
        }  
    }

    // Copy Assignment Operator
    KeyBy_Emitter &operator=(const KeyBy_Emitter &_other)
    {
        if (this != &_other) {
            key_extr = _other.key_extr;
            num_dests = _other.num_dests;
            size = _other.size;
            useTreeMode = _other.useTreeMode;
            for (auto *b: batches_output) {
                if (b != nullptr) {
                    delete b;
                }
            }
            batches_output.clear();
            batches_output.insert(batches_output.end(), num_dests, nullptr);
            execution_mode = _other.execution_mode;
            last_time_punct = _other.last_time_punct;
            delivered = _other.delivered;
            received_inputs = _other.received_inputs;
            last_sent_wms = _other.last_sent_wms;
        }
        return *this;
    }

    // Move Assignment Operator
    KeyBy_Emitter &operator=(KeyBy_Emitter &&_other)
    {
        key_extr = std::move(_other.key_extr);
        num_dests = _other.num_dests;
        size = _other.size;
        useTreeMode = _other.useTreeMode;
        output_queue = std::move(_other.output_queue);
        for (auto *b: batches_output) {
            if (b != nullptr) {
                delete b;
            }
        }
        batches_output = std::move(_other.batches_output);
        if (size == 0) { // delete all the Single_t items in the recycling queue
            if (queue != nullptr) {
                Single_t<decltype(get_tuple_t_KeyExtr(key_extr))> *del_single = nullptr;
                while (queue->pop((void **) &del_single)) {
                    delete del_single;
                }
                delete queue; // delete the recycling queue
            }
        }
        else { // delete all the batches in the recycling queue
            if (queue != nullptr) {
                Batch_t<decltype(get_tuple_t_KeyExtr(key_extr))> *del_batch = nullptr;
                while (queue->pop((void **) &del_batch)) {
                    delete del_batch;
                }
                delete queue; // delete the recycling queue
            }
        }
        queue = std::exchange(_other.queue, nullptr);
        execution_mode = _other.execution_mode;
        last_time_punct = _other.last_time_punct;
        delivered = std::move(_other.delivered);
        received_inputs = _other.received_inputs;
        last_sent_wms = std::move(_other.last_sent_wms);
        return *this;
    }

    // Create a clone of the emitter
    Basic_Emitter *clone() const override
    {
        KeyBy_Emitter<key_extractor_func_t> *copy = new KeyBy_Emitter<key_extractor_func_t>(*this);
        return copy;
    }

    // Get the number of destinations of the emitter
    size_t getNumDestinations() const override
    {
        return num_dests;
    }

    // Set the emitter to work in tree-based mode
    void setTreeMode(bool _useTreeMode) override
    {
        useTreeMode = _useTreeMode;
    }

    // Get a reference to the vector of output messages used by the emitter
    std::vector<std::pair<void *, size_t>> &getOutputQueue() override
    {
        return output_queue;
    }

    // Emit method (non in-place version)
    void emit(void *_out,
              uint64_t _identifier,
              uint64_t _timestamp,
              uint64_t _watermark,
              ff::ff_monode *_node) override
    {
        received_inputs++;
        decltype(get_tuple_t_KeyExtr(key_extr)) *tuple = reinterpret_cast<decltype(get_tuple_t_KeyExtr(key_extr)) *>(_out);
        if (size == 0) { // no batching
            Single_t<decltype(get_tuple_t_KeyExtr(key_extr))> *output = allocateSingle_t(std::move(*tuple), _identifier, _timestamp, _watermark, queue);
            routing(output, _node);
        }
        else { // batching
            routing_batched(*tuple, _timestamp, _watermark, _node);
        }
    }

    // Emit method (in-place version)
    void emit_inplace(void *_out,
                      ff::ff_monode *_node) override
    {
        received_inputs++;
        Single_t<decltype(get_tuple_t_KeyExtr(key_extr))> *output = reinterpret_cast<Single_t<decltype(get_tuple_t_KeyExtr(key_extr))> *>(_out);
        if (size == 0) { // no batching
            routing(output, _node);
        }
        else { // batching
            routing_batched(output->tuple, output->getTimestamp(), output->getWatermark(), _node);
            deleteSingle_t(output); // delete the input Single_t
        }
    }

    // Routing method
    void routing(Single_t<tuple_t> *_output,
                 ff::ff_monode *_node)
    {
        if ((execution_mode == Execution_Mode_t::DEFAULT) && (received_inputs % WF_DEFAULT_WM_AMOUNT == 0)) { // check punctuaction generation logic
            generate_punctuation(_output->getWatermark(), _node);
        }
        auto key = key_extr(_output->tuple); // extract the key attribute of the tuple
        size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
        size_t dest_id = hashcode % num_dests; // compute the destination identifier associated with the key attribute
        assert(last_sent_wms[dest_id] <= _output->getWatermark()); // sanity check
        last_sent_wms[dest_id] = _output->getWatermark(); // save the last watermark emitted to this destination
        if (!useTreeMode) { // real send
            _node->ff_send_out_to(_output, dest_id);
            delivered[dest_id]++;
        }
        else { // output is buffered
            output_queue.push_back(std::make_pair(_output, dest_id));
            delivered[dest_id]++;
        }
    }

    // Routing method to be used in batched mode
    void routing_batched(tuple_t &_tuple,
                         uint64_t _timestamp,
                         uint64_t _watermark,
                         ff::ff_monode *_node)
    {
        if ((execution_mode == Execution_Mode_t::DEFAULT) && (received_inputs % WF_DEFAULT_WM_AMOUNT == 0)) { // check punctuaction generation logic
            generate_punctuation(_watermark, _node);
        }
        auto key = key_extr(_tuple); // extract the key attribute of the tuple
        size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
        size_t dest_id = hashcode % num_dests; // compute the destination identifier associated with the key attribute
        if (batches_output[dest_id] == nullptr) { // the batch must be allocated
            batches_output[dest_id] = allocateBatch_CPU_t<decltype(get_tuple_t_KeyExtr(key_extr))>(size, queue);
        }
        batches_output[dest_id]->addTuple(std::move(_tuple), _timestamp, _watermark);
        if (batches_output[dest_id]->getSize() == size) { // batch is complete and must be sent
            assert(last_sent_wms[dest_id] <= batches_output[dest_id]->getWatermark()); // sanity check
            last_sent_wms[dest_id] = batches_output[dest_id]->getWatermark(); // save the last watermark emitted to this destination
            if (!useTreeMode) { // real send
                _node->ff_send_out_to(batches_output[dest_id], dest_id);
                delivered[dest_id]++;
            }
            else { // output is buffered
                output_queue.push_back(std::make_pair(batches_output[dest_id], dest_id));
                delivered[dest_id]++;
            }
            batches_output[dest_id] = nullptr;
        }
    }

    // Punctuation propagation method
    void propagate_punctuation(uint64_t _watermark,
                               ff::ff_monode * _node) override
    {
        flush(_node); // flush all the internal partially filled batches (if any)
        if (size == 0) { // no batching
            tuple_t t; // create an empty tuple
            Single_t<decltype(get_tuple_t_KeyExtr(key_extr))> *punc = allocateSingle_t(std::move(t), 0, 0, _watermark, queue);
            (punc->delete_counter).fetch_add(num_dests-1);
            assert((punc->fields).size() == 3); // sanity check
            (punc->fields).insert((punc->fields).end(), num_dests-1, (punc->fields)[2]); // copy the watermark (having one per destination)
            punc->isPunctuation = true;
            for (size_t i=0; i<num_dests; i++) {
                assert(last_sent_wms[i] <= _watermark); // sanity check
                last_sent_wms[i] = _watermark; // save the last watermark emitted to this destination
                if (!useTreeMode) { // real send
                    _node->ff_send_out_to(punc, i);
                }
                else { // punctuation is buffered
                    output_queue.push_back(std::make_pair(punc, i));
                }
            }
        }
        else { // batching
            tuple_t t; // create an empty tuple
            Batch_CPU_t<decltype(get_tuple_t_KeyExtr(key_extr))> *punc = allocateBatch_CPU_t<decltype(get_tuple_t_KeyExtr(key_extr))>(size, queue);
            punc->addTuple(std::move(t), 0, _watermark);
            (punc->delete_counter).fetch_add(num_dests-1);
            assert((punc->watermarks).size() == 1); // sanity check
            (punc->watermarks).insert((punc->watermarks).end(), num_dests-1, (punc->watermarks)[0]); // copy the watermark (having one per destination)
            punc->isPunctuation = true;
            for (size_t i=0; i<num_dests; i++) {
                assert(last_sent_wms[i] <= _watermark); // sanity check
                last_sent_wms[i] = _watermark; // save the last watermark emitted to this destination
                if (!useTreeMode) { // real send
                    _node->ff_send_out_to(punc, i);
                }
                else { // punctuation is buffered
                    output_queue.push_back(std::make_pair(punc, i));
                }
            }
        }
    }

    // Punctuation generation method
    void generate_punctuation(uint64_t _watermark,
                              ff::ff_monode *_node)
    {
        if (current_time_usecs() - last_time_punct >= WF_DEFAULT_WM_INTERVAL_USEC) { // check the end of the sample
            std::vector<int> idxs;
            for (size_t i=0; i<num_dests; i++) { // select the destinations receiving the new punctuation
                if (delivered[i] == 0) {
                    if (size == 0) { // no batching
                        idxs.push_back(i);
                    }
                    else { // batching
                        if (batches_output[i] != nullptr) {
                            assert(batches_output[i]->getSize() > 0); // sanity check
                            assert(last_sent_wms[i] <= batches_output[i]->getWatermark()); // sanity check
                            last_sent_wms[i] = batches_output[i]->getWatermark(); // save the last watermark emitted to this destination
                            if (!useTreeMode) { // real send
                                _node->ff_send_out_to(batches_output[i], i);
                            }
                            else { // output is buffered
                                output_queue.push_back(std::make_pair(batches_output[i], i));
                            }
                            batches_output[i] = nullptr;
                        }
                        idxs.push_back(i);
                    }
                }
                else {
                    delivered[i] = 0;
                }
            }
            if (idxs.size() == 0) {
                return;
            }
            if (size == 0) { // no batching
                tuple_t t; // create an empty tuple
                Single_t<decltype(get_tuple_t_KeyExtr(key_extr))> *punc = allocateSingle_t(std::move(t), 0, 0, _watermark, queue);
                (punc->delete_counter).fetch_add(idxs.size()-1);
                assert((punc->fields).size() == 3); // sanity check
                (punc->fields).insert((punc->fields).end(), num_dests-1, (punc->fields)[2]); // copy the watermark (having one per destination)
                punc->isPunctuation = true;
                for (auto id: idxs) {
                    assert(last_sent_wms[id] <= _watermark); // sanity check
                    last_sent_wms[id] = _watermark; // save the last watermark emitted to this destination
                    if (!useTreeMode) { // real send
                        _node->ff_send_out_to(punc, id);
                    }
                    else { // punctuation is buffered
                        output_queue.push_back(std::make_pair(punc, id));
                    }
                }
            }
            else { // batching
                tuple_t t; // create an empty tuple
                Batch_CPU_t<decltype(get_tuple_t_KeyExtr(key_extr))> *punc = allocateBatch_CPU_t<decltype(get_tuple_t_KeyExtr(key_extr))>(size, queue);
                punc->addTuple(std::move(t), 0, _watermark);
                (punc->delete_counter).fetch_add(idxs.size()-1);
                assert((punc->watermarks).size() == 1); // sanity check
                (punc->watermarks).insert((punc->watermarks).end(), num_dests-1, (punc->watermarks)[0]); // copy the watermark (having one per destination)
                punc->isPunctuation = true;
                for (auto id: idxs) {
                    assert(last_sent_wms[id] <= _watermark); // sanity check
                    last_sent_wms[id] = _watermark; // save the last watermark emitted to this destination
                    if (!useTreeMode) { // real send
                        _node->ff_send_out_to(punc, id);
                    }
                    else { // output is buffered
                        output_queue.push_back(std::make_pair(punc, id));
                    }
                }
            }
            last_time_punct = current_time_usecs();
        }
    }

    // Flushing method
    void flush(ff::ff_monode *_node) override
    {
        if (size > 0) { // only batching
            for (size_t i=0; i<num_dests; i++) {
                if (batches_output[i] != nullptr) {
                    assert(batches_output[i]->getSize() > 0); // sanity check
                    assert(last_sent_wms[i] <=  batches_output[i]->getWatermark()); // sanity check
                    last_sent_wms[i] = batches_output[i]->getWatermark(); // save the last watermark emitted to this destination
                    if (!useTreeMode) { // real send
                        _node->ff_send_out_to(batches_output[i], i);
                        delivered[i]++;
                    }
                    else { // output is buffered
                        output_queue.push_back(std::make_pair(batches_output[i], i));
                        delivered[i]++;
                    }
                    batches_output[i] = nullptr;
                }
            }
        }
    }
};

} // namespace wf

#endif
