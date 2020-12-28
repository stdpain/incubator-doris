#include "runtime/runtime_filter_mgr.h"

#include "exprs/runtime_filter.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/plan_fragment_executor.h"

namespace doris {

RuntimeFilterMgr::RuntimeFilterMgr(PlanFragmentExecutor* plan_fragment_executor)
        : _mgr_type(false), _plan_fragment_executor(plan_fragment_executor) {}

RuntimeFilterMgr::RuntimeFilterMgr() : _mgr_type(true) {}

RuntimeFilterMgr::~RuntimeFilterMgr() {}

Status RuntimeFilterMgr::get_filter_by_role(const UniqueId& query_id, const int filter_id,
                                            const int role, ShuffleRuntimeFilter** target) {
    std::string key = query_id.to_string() + "-" + std::to_string(filter_id);
    auto iter = _filter_map.find(key);
    if (iter == _filter_map.end()) {
        return Status::InvalidArgument("unknown filter");
    }
    if (iter->second.role != role) {
        return Status::InvalidArgument("not expect role");
    }
    *target = iter->second.filter;
    return Status::OK();
}

Status RuntimeFilterMgr::get_consume_filter(const UniqueId& query_id, const int filter_id,
                                            ShuffleRuntimeFilter** consumer_filter) {
    return get_filter_by_role(query_id, filter_id, ROLE_CONSUMER, consumer_filter);
}

Status RuntimeFilterMgr::get_producer_filter(const UniqueId& query_id, const int filter_id,
                                             ShuffleRuntimeFilter** producer_filter) {
    return get_filter_by_role(query_id, filter_id, ROLE_PRODUCER, producer_filter);
}

Status RuntimeFilterMgr::regist_filter(const UniqueId& query_id, const int role,
                                       const TRuntimeFilterDesc& desc) {
    if (!_mgr_type) {
        std::string key = query_id.to_string() + "-" + std::to_string(desc.filter_id);
        auto iter = _filter_map.find(key);
        if (iter != _filter_map.end()) {
            return Status::InvalidArgument("filter has registed");
        }
        RuntimeFilterMgrVal filter_mgr_val;
        filter_mgr_val.create_time = UnixMillis();
        filter_mgr_val.state = _plan_fragment_executor->runtime_state();
        filter_mgr_val.pool = _plan_fragment_executor->obj_pool();
        filter_mgr_val.tracker = filter_mgr_val.state->fragment_mem_tracker().get();
        filter_mgr_val.filter = filter_mgr_val.pool->add(new ShuffleRuntimeFilter());
        _filter_map.emplace(key, filter_mgr_val);
    } else {
    }
    return Status::OK();
}

Status RuntimeFilterMgr::update_filter(const PPublishFilterRequest* request, const char* data) {
    struct UpdateRuntimeFilterParams params;
    params.publish_request = request;
    params.data = data;
    int filter_id = request->filter_id();
    ShuffleRuntimeFilter* real_filter = nullptr;
    Status status = get_consume_filter(request->query_id(), filter_id, &real_filter);
    if (!status.ok()) {
        return status;
    }
    return real_filter->update_filter(&params);
}

} // namespace doris