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
 *  @file    shipper.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Shipper class used to send outputs by the FlatMap operator
 *  
 *  @section Shipper (Description)
 *  
 *  This file implements the Shipper class used to send outputs to the
 *  next stage of the application. It is used by the FlatMap operator.
 */ 

#ifndef SHIPPER_H
#define SHIPPER_H

/// includes
#include<ff/multinode.hpp>
#include<single_t.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<basic_emitter.hpp>

namespace wf {

/** 
 *  \class Shipper
 *  
 *  \brief Shipper class used to send outputs by the FlatMap operator
 *  
 *  This class implements the Shipper object to send outputs to the next
 *  stage of the application. It is used by the FlatMap operator.
 */ 
template<typename result_t>
class Shipper
{
private:
    template<typename T1> friend class FlatMap_Replica; // friendship with FlatMap_Replica class
    Basic_Emitter *emitter; // pointer to the emitter used for the delivery of messages
    ff::ff_monode *node; // pointer to the fastflow node to be passed to the emitter
    uint64_t num_delivered; // counter of the delivered results
    uint64_t timestamp; // timestamp to be used for sending messages
    uint64_t watermark; // watermark to be used for sending messages
#if defined (WF_TRACING_ENABLED)
    Stats_Record *stats_record = nullptr;

    // Set the pointer to the Stats_Record object
    void setStatsRecord(Stats_Record *_stats_record)
    {
        stats_record = _stats_record;
    }
#endif

    // Constructor
    Shipper(Basic_Emitter *_emitter,
            ff::ff_monode *_node):
            emitter(_emitter),
            node(_node),
            num_delivered(0),
            timestamp(0),
            watermark(0) {}

    // Copy Constructor
    Shipper(const Shipper &_other):
            node(_other.node),
            num_delivered(_other.num_delivered),
            timestamp(_other.timestamp),
            watermark(_other.watermark)
    {
        if (_other.emitter != nullptr) {
            emitter = (_other.emitter)->clone();
        }
        else {
            emitter = nullptr;
        }
#if defined (WF_TRACING_ENABLED)
        stats_record = _other.stats_record;
#endif
    }

    // Move Constructor
    Shipper(Shipper &&_other):
            emitter(std::exchange(_other.emitter, nullptr)),
            node(std::exchange(_other.node, nullptr)),
            num_delivered(_other.num_delivered),
            timestamp(_other.timestamp),
            watermark(_other.watermark)
    {
#if defined (WF_TRACING_ENABLED)
        stats_record = std::exchange(_other.stats_record, nullptr);
#endif
    }

    // Destructor
    ~Shipper()
    {
        if (emitter != nullptr) {
            delete emitter;
        }
    }

    // Copy Assignment Operator
    Shipper &operator=(const Shipper &_other)
    {
        if (this != &_other) {
            if (emitter != nullptr) {
                delete emitter;
            }
            if (_other.emitter != nullptr) {
                emitter = (_other.emitter)->clone();
            }
            else {
                emitter = nullptr;
            }
            node = _other.node;
            num_delivered = _other.num_delivered;
            timestamp = _other.timestamp;
            watermark = _other.watermark;
#if defined (WF_TRACING_ENABLED)
            stats_record = _other.stats_record;
#endif
        }
        return *this;
    }

    // Move Assignment Operator
    Shipper &operator=(Shipper &&_other)
    {
        if (emitter != nullptr) {
            delete emitter;
        }
        emitter = std::exchange(_other.emitter, nullptr);
        node = std::exchange(_other.node, nullptr);
        num_delivered = _other.num_delivered;
        timestamp = _other.timestamp;
        watermark = _other.watermark;
#if defined (WF_TRACING_ENABLED)
        stats_record = std::exchange(_other.stats_record, nullptr);
#endif
        return *this;
    }

    // Set the configuration parameters
    void setShipperParameters(uint64_t _ts, uint64_t _wm)
    {
        timestamp = _ts;
        watermark = _wm;
    }

    // Flushing function of the shipper
    void flush()
    {
        emitter->flush(node); // call the flush of the emitter
    }

public:
    /** 
     *  \brief Get the number of results delivered by the Shipper
     *  
     *  \return number of results
     */ 
    uint64_t getNumDelivered() const
    {
        return num_delivered;
    }

    /** 
     *  \brief Deliver a result
     *  
     *  \param _r result to be delivered (copy semantics)
     */ 
    void push(const result_t &_r)
    {
        result_t copy_result = _r; // copy of the result to be delivered
        emitter->emit(&copy_result, 0, timestamp, watermark, node);
        num_delivered++;
#if defined (WF_TRACING_ENABLED)
        assert(stats_record != nullptr);
        stats_record->outputs_sent++;
        stats_record->bytes_sent += sizeof(result_t);
#endif
    }

    /** 
     *  \brief Deliver a result
     *  
     *  \param _r result to be delivered (move semantics)
     */ 
    void push(result_t &&_r)
    {
        emitter->emit(&_r, 0, timestamp, watermark, node);
        num_delivered++;
#if defined (WF_TRACING_ENABLED)
        assert(stats_record != nullptr);
        stats_record->outputs_sent++;
        stats_record->bytes_sent += sizeof(result_t);
#endif
    }
};

} // namespace wf

#endif
