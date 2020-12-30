#include "runtime/runtime_filter_mgr.h"

#include <string>

#include "client_cache.h"
#include "exprs/runtime_filter.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/mem_tracker.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "service/brpc.h"
#include "util/time.h"

namespace doris {

RuntimeFilterMgr::RuntimeFilterMgr(const UniqueId& query_id, RuntimeState* state) : _state(state) {}

RuntimeFilterMgr::~RuntimeFilterMgr() {}

Status RuntimeFilterMgr::init() {
    DCHECK(_state->instance_mem_tracker().get() != nullptr);
    _tracker = _state->instance_mem_tracker().get();
    return Status::OK();
}

Status RuntimeFilterMgr::get_filter_by_role(const int filter_id, const int role,
                                            ShuffleRuntimeFilter** target) {
    std::string key = std::to_string(filter_id);
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

Status RuntimeFilterMgr::get_consume_filter(const int filter_id,
                                            ShuffleRuntimeFilter** consumer_filter) {
    return get_filter_by_role(filter_id, ROLE_CONSUMER, consumer_filter);
}

Status RuntimeFilterMgr::get_producer_filter(const int filter_id,
                                             ShuffleRuntimeFilter** producer_filter) {
    return get_filter_by_role(filter_id, ROLE_PRODUCER, producer_filter);
}

Status RuntimeFilterMgr::regist_filter(const int role, const TRuntimeFilterDesc& desc) {
    std::string key = std::to_string(desc.filter_id);
    auto iter = _filter_map.find(key);
    if (iter != _filter_map.end()) {
        return Status::InvalidArgument("filter has registed");
    }
    RuntimeFilterMgrVal filter_mgr_val;
    filter_mgr_val.role = role;
    filter_mgr_val.runtime_filter_desc = &desc;
    filter_mgr_val.filter = _pool.add(new ShuffleRuntimeFilter(_state, _tracker, &_pool));
    filter_mgr_val.filter->set_role(role);
    filter_mgr_val.filter->init_with_desc(&desc);
    _filter_map.emplace(key, filter_mgr_val);
    return Status::OK();
}

Status RuntimeFilterMgr::update_filter(const PPublishFilterRequest* request, const char* data) {
    UpdateRuntimeFilterParams params;
    params.publish_request = request;
    params.data = data;
    int filter_id = request->filter_id();
    ShuffleRuntimeFilter* real_filter = nullptr;
    RETURN_IF_ERROR(get_consume_filter(filter_id, &real_filter));
    return real_filter->update_filter(&params);
}

void RuntimeFilterMgr::set_runtime_filter_params(
        const TPlanFragmentRuntimeFiltersParams& runtime_filter_params) {
    this->_runtime_filter_params = runtime_filter_params;
}

Status RuntimeFilterMgr::get_merge_addr(TNetworkAddress* addr) {
    *addr = this->_runtime_filter_params.coord_merge_addr;
    return Status::OK();
}

Status RuntimeFilterMergeController::init_with_desc(const TRuntimeFilterDesc* runtime_filter_desc) {
    std::lock_guard<std::mutex> guard(_filter_map_mutex);
    RuntimeFilterCntlVal cntVal;
    cntVal.runtime_filter_desc = runtime_filter_desc;
    cntVal.pool.reset(new ObjectPool());
    cntVal.tracker = MemTracker::CreateTracker();
    cntVal.filter = cntVal.pool->add(
            new ShuffleRuntimeFilter(nullptr, cntVal.tracker.get(), cntVal.pool.get()));
    std::string filter_id = std::to_string(runtime_filter_desc->filter_id);
    LOG(WARNING) << "regist filter id:" << filter_id << " this:" << (void*)this;
    cntVal.filter->init_with_desc(runtime_filter_desc);
    _filter_map.emplace(filter_id, cntVal);
    return Status::OK();
}

Status RuntimeFilterMergeController::init_from(std::vector<TPlanNode> nodes) {
    for (auto& node : nodes) {
        if (node.node_type == TPlanNodeType::HASH_JOIN_NODE) {
            for (auto& filter : node.runtime_filters) {
                init_with_desc(&filter);
            }
        }
    }
    return Status::OK();
}

// merge data
Status RuntimeFilterMergeController::merge(const PMergeFilterRequest* request, const char* data) {
    auto iter = _filter_map.find(std::to_string(request->filter_id()));
    LOG(WARNING) << "recv filter id:" << request->filter_id() << " this:" << (void*)this;
    if (iter == _filter_map.end()) {
        LOG(WARNING) << "unknown filter id:" << std::to_string(request->filter_id());
        return Status::InvalidArgument("not found filter id");
    }
    MergeRuntimeFilterParams params;
    params.data = data;
    params.merge_request = request;
    std::shared_ptr<MemTracker> tracker = MemTracker::CreateTracker();
    ObjectPool pool;
    ShuffleRuntimeFilter runtime_filter(nullptr, tracker.get(), &pool);
    RETURN_IF_ERROR(runtime_filter.init_from_params(&params));
    RETURN_IF_ERROR(iter->second.filter->merge_from(runtime_filter));
    iter->second.arrive_id.insert(std::to_string(UnixMicros()));
    if (iter->second.arrive_id.size() == 2) {
        for (const auto& kv : this->_runtimefilter_params.planid_to_addr) {
            // kv.second
            brpc::Controller cntl;
            brpc::Channel channel;
            if (channel.Init(kv.second.hostname.c_str(), kv.second.port, nullptr) != 0) {
                LOG(WARNING) << "channel init err";
                continue;
            }
            PPublishFilterRequest apply_request;
            apply_request.set_filter_id(request->filter_id());
            apply_request.set_filter_type(PPublishFilterRequest_FilterType_BLOOM_FILTER);
            *apply_request.mutable_query_id() = request->query_id();
            auto bloom_filter = apply_request.mutable_bloom_filter();
            PPublishFilterResponse apply_response;
            PBackendService_Stub stub(&channel);
            void* data = nullptr;
            int len = 0;
            iter->second.filter->get_data(&data, &len);
            bloom_filter->set_filter_length(len);
            bloom_filter->set_always_true(false);
            cntl.request_attachment().append(data, len);
            stub.apply_filter(&cntl, &apply_request, &apply_response, nullptr);
            if (cntl.Failed()) {
                LOG(WARNING) << "RPC apply_filter ERR:" << cntl.ErrorText();
            }
        }
    }
    return Status::OK();
}
} // namespace doris