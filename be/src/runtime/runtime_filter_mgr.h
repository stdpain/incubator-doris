// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <thread>

#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/runtime_filter.h"
#include "util/time.h"
#include "util/uid_util.h"
// defination for TRuntimeFilterDesc
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {
class TUniqueId;
class RuntimeFilter;
class FragmentExecState;
class PlanFragmentExecutor;
class PPublishFilterRequest;
class PMergeFilterRequest;

/// producer:
/// Filter filter;
/// get_filter(filter_id, &filter);
/// filter->merge(origin_filter)

/// comsumer
/// get_filter(filter_id, &filter)
/// filter->wait
/// if filter->ready().ok(), use filter

#define ROLE_PRODUCER 0
#define ROLE_CONSUMER 1
#define ROLE_MERGER 2

// owned by RuntimeState
class RuntimeFilterMgr {
public:
    RuntimeFilterMgr(const UniqueId& query_id, RuntimeState* state);

    ~RuntimeFilterMgr();

    Status init();

    // get_filter is used in
    Status get_consume_filter(const int filter_id, ShuffleRuntimeFilter** consumer_filter);

    Status get_producer_filter(const int filter_id, ShuffleRuntimeFilter** producer_filter);
    // regist filter
    Status regist_filter(const int role, const TRuntimeFilterDesc& desc);

    // update filter by remote
    Status update_filter(const PPublishFilterRequest* request, const char* data);

    void set_runtime_filter_params(const TPlanFragmentRuntimeFiltersParams& runtime_filter_params);

    Status get_merge_addr(TNetworkAddress* addr);

private:
    Status get_filter_by_role(const int filter_id, const int role, ShuffleRuntimeFilter** target);

    struct RuntimeFilterMgrVal {
        int role; // reference to ROLE_*
        const TRuntimeFilterDesc* runtime_filter_desc;
        ShuffleRuntimeFilter* filter;
    };
    // we use _filter_map_mutex protect _filter_map
    // RuntimeFilterMgr is owned by RuntimeState, so we only
    // use filter_id as key
    // key: "filter-id"
    std::map<std::string, RuntimeFilterMgrVal> _filter_map;
    std::mutex _filter_map_mutex;

    // we use a single thread to scan time out filter
    // std::mutex _thr_mutex;
    // std::unique_ptr<std::thread> scan_thr;

    RuntimeState* _state;
    MemTracker* _tracker;
    ObjectPool _pool;

    TPlanFragmentRuntimeFiltersParams _runtime_filter_params;
};

class RuntimeFilterMergeController {
public:
    RuntimeFilterMergeController() = default;
    ~RuntimeFilterMergeController() = default;

    Status init_with_desc(const TRuntimeFilterDesc* runtime_filter_desc);

    Status init_from(std::vector<TPlanNode> nodes);

    Status merge(const PMergeFilterRequest* request, const char* data);

    void set_filter_params(const TPlanFragmentRuntimeFiltersParams& params) {
        _runtimefilter_params = params;
    }

private:
    struct RuntimeFilterCntlVal {
        int64_t create_time;
        const TRuntimeFilterDesc* runtime_filter_desc;
        ShuffleRuntimeFilter* filter;
        std::set<std::string> arrive_id; // fragment_id ?
        std::shared_ptr<MemTracker> tracker;
        std::shared_ptr<ObjectPool> pool;
    };
    // filter-id -> val
    std::mutex _filter_map_mutex;
    std::map<std::string, RuntimeFilterCntlVal> _filter_map;
    TPlanFragmentRuntimeFiltersParams _runtimefilter_params;
};

} // namespace doris
