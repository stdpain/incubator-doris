#pragma once
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <thread>

#include "common/status.h"
#include "exprs/runtime_filter.h"
#include "util/time.h"
#include "util/uid_util.h"
// defination for TRuntimeFilterDesc
#include "gen_cpp/PaloInternalService_types.h"

namespace doris {
class TUniqueId;
class RuntimeFilter;
class FragmentExecState;
class PlanFragmentExecutor;
class PPublishFilterRequest;

/// producer:
/// unique_ptr<Filter> filter;
/// get_filter(filter_id, &filter);
/// filter->merge(origin_filter)

/// comsumer
/// get_filter(filter_id, &filter)
/// filter_wait
/// if filter->ready(), use filter

#define ROLE_PRODUCER 0
#define ROLE_CONSUMER 1
#define ROLE_MERGER 2

class RuntimeFilterMgr {
public:
    //
    RuntimeFilterMgr(PlanFragmentExecutor* plan_fragment_executor);
    // default Runtime Filter Mgr is a 'global mgr'
    RuntimeFilterMgr();
    ~RuntimeFilterMgr();

    // get_filter is used in
    Status get_consume_filter(const UniqueId& query_id, const int filter_id,
                              ShuffleRuntimeFilter** consumer_filter);

    Status get_producer_filter(const UniqueId& query_id, const int filter_id,
                               ShuffleRuntimeFilter** producer_filter);
    // regist filter
    Status regist_filter(const UniqueId& query_id, const int role, const TRuntimeFilterDesc& desc);

    Status update_filter(const PPublishFilterRequest* request, const char* data);

private:
    Status get_filter_by_role(const UniqueId& query_id, const int filter_id, const int role,
                              ShuffleRuntimeFilter** target);
    struct RuntimeFilterMgrVal {
        int64_t create_time;
        int role; // reference to ROLE_*
        TRuntimeFilterDesc *runtime_filter_desc;
        ShuffleRuntimeFilter* filter;
        RuntimeState* state;
        ObjectPool* pool;
        MemTracker* tracker;
    };
    // we use _filter_map_mutex protect _filter_map
    // key: "unique-id - filter-id"
    // this map is used for merger
    std::map<std::string, RuntimeFilterMgrVal> _filter_map;
    std::mutex _filter_map_mutex;
    std::condition_variable _filter_map_cv;

    // we use a single thread to scan time out filter
    // std::mutex _thr_mutex;
    // std::unique_ptr<std::thread> scan_thr;

    // if is mgr_type == true mgr is a 'global mgr' ,it was used by runtime filter merge service
    //  state,object pool,tracker is owned by this
    // else state,object pool,tracker is from _plan_fragment_executor
    bool _mgr_type;

    PlanFragmentExecutor* _plan_fragment_executor;
};

} // namespace doris
