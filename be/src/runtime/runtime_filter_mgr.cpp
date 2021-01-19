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
        LOG(WARNING) << "unknown filter...:" << key;
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
    params.request = request;
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

Status RuntimeFilterMergeControllerEntity::_init_with_desc(
        const TRuntimeFilterDesc* runtime_filter_desc) {
    std::lock_guard<std::mutex> guard(_filter_map_mutex);
    std::shared_ptr<RuntimeFilterCntlVal> cntVal = std::make_shared<RuntimeFilterCntlVal>();
    // We don’t know when runtime_filter_desc will be released,
    // so we need to copy to cntVal
    cntVal->runtime_filter_desc = *runtime_filter_desc;
    cntVal->pool.reset(new ObjectPool());
    cntVal->tracker = MemTracker::CreateTracker();
    cntVal->filter = cntVal->pool->add(
            new ShuffleRuntimeFilter(nullptr, cntVal->tracker.get(), cntVal->pool.get()));

    std::string filter_id = std::to_string(runtime_filter_desc->filter_id);
    LOG(INFO) << "entity filter id:" << filter_id;
    cntVal->filter->init_with_desc(&cntVal->runtime_filter_desc);
    _filter_map.emplace(filter_id, cntVal);
    return Status::OK();
}

Status RuntimeFilterMergeControllerEntity::init_from(UniqueId query_id,
                                                     const std::vector<TPlanNode>& nodes) {
    _query_id = query_id;
    for (auto& node : nodes) {
        if (node.node_type == TPlanNodeType::HASH_JOIN_NODE) {
            for (auto& filter : node.runtime_filters) {
                _init_with_desc(&filter);
            }
        }
    }
    return Status::OK();
}

// merge data
Status RuntimeFilterMergeControllerEntity::merge(const PMergeFilterRequest* request,
                                                 const char* data) {
    std::shared_ptr<RuntimeFilterCntlVal> cntVal;
    int merged_size = 0;
    {
        std::lock_guard<std::mutex> guard(_filter_map_mutex);
        auto iter = _filter_map.find(std::to_string(request->filter_id()));
        LOG(INFO) << "recv filter id:" << request->filter_id();
        if (iter == _filter_map.end()) {
            LOG(WARNING) << "unknown filter id:" << std::to_string(request->filter_id());
            return Status::InvalidArgument("unknown filter id");
        }
        cntVal = iter->second;
        MergeRuntimeFilterParams params;
        params.data = data;
        params.request = request;
        std::shared_ptr<MemTracker> tracker = iter->second->tracker;
        ObjectPool pool;
        ShuffleRuntimeFilter runtime_filter(nullptr, tracker.get(), &pool);
        RETURN_IF_ERROR(runtime_filter.init_with_proto_param(&params));
        RETURN_IF_ERROR(cntVal->filter->merge_from(runtime_filter));
        cntVal->arrive_id.insert(std::to_string(UnixMicros()));
        merged_size = cntVal->arrive_id.size();
    }

    std::set<std::string> endpoints;
    for (auto& entity : this->_runtimefilter_params.planid_to_addr) {
        endpoints.insert(entity.second.hostname + std::to_string(entity.second.port));
    }
    LOG(INFO) << "merge size:" << merged_size << ",endpoint size:" << endpoints.size();
    if (merged_size == endpoints.size()) {
        for (const auto& kv : this->_runtimefilter_params.planid_to_addr) {
            // kv.second
            brpc::Controller cntl;
            brpc::Channel channel;
            if (channel.Init(kv.second.hostname.c_str(), kv.second.port, nullptr) != 0) {
                LOG(WARNING) << "channel init err";
                continue;
            }
            PPublishFilterRequest apply_request;
            PPublishFilterResponse apply_response;
            apply_request.set_filter_id(request->filter_id());
            PBackendService_Stub stub(&channel);
            if (cntVal->filter->type() == RuntimeFilterType::BLOOM_FILTER) {
                apply_request.set_filter_type(PFilterType::BLOOM_FILTER);
                *apply_request.mutable_query_id() = request->query_id();
                void* data = nullptr;
                int len = 0;
                cntVal->filter->serialize(&apply_request, &data, &len);
                cntl.request_attachment().append(data, len);
            } else if (cntVal->filter->type() == RuntimeFilterType::MINMAX_FILTER) {
                apply_request.set_filter_type(PFilterType::MINMAX_FILTER);
                *apply_request.mutable_query_id() = request->query_id();
                apply_request.mutable_minmax_filter()->set_column_type(
                        PColumnType::COLUMN_TYPE_BIGINT);
                cntVal->filter->serialize(&apply_request, nullptr, nullptr);
            }
            stub.apply_filter(&cntl, &apply_request, &apply_response, nullptr);
            LOG(WARNING) << "apply_request:" << apply_request.ShortDebugString();
            if (cntl.Failed()) {
                LOG(WARNING) << "RPC apply_filter ERR:" << cntl.ErrorText();
            }
        }
    }
    // TODO: clear the cntl
    return Status::OK();
}

Status RuntimeFilterMergeController::add_entity(
        const TExecPlanFragmentParams& params,
        std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle) {
    runtime_filter_merge_entity_closer entity_closer =
            std::bind(runtime_filter_merge_entity_close, this, std::placeholders::_1);

    std::lock_guard<std::mutex> guard(_controller_mutex);
    UniqueId query_id(params.params.query_id);
    std::string query_id_str = query_id.to_string();
    auto iter = _filter_controller_map.find(query_id_str);

    if (iter == _filter_controller_map.end()) {
        *handle = std::shared_ptr<RuntimeFilterMergeControllerEntity>(
                new RuntimeFilterMergeControllerEntity(), entity_closer);
        _filter_controller_map[query_id_str] = handle->get();
    } else {
        *handle = _filter_controller_map[query_id_str]->shared_from_this();
    }
    LOG(WARNING) << "add entity, query-id:" << query_id_str;

    std::shared_ptr<RuntimeFilterMergeControllerEntity>& filter_merge_controller = *handle;
    // params.fragment.plan.nodes
    RETURN_IF_ERROR(filter_merge_controller->init_from(query_id, params.fragment.plan.nodes));
    filter_merge_controller->set_filter_params(params.params.runtime_filter_params);
    return Status::OK();
}

Status RuntimeFilterMergeController::acquire(
        UniqueId query_id, std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle) {
    std::lock_guard<std::mutex> guard(_controller_mutex);
    std::string query_id_str = query_id.to_string();
    auto iter = _filter_controller_map.find(query_id_str);
    if (iter == _filter_controller_map.end()) {
        LOG(WARNING) << "not found entity, query-id:" << query_id_str;
        return Status::InvalidArgument("not found entity");
    }
    *handle = _filter_controller_map[query_id_str]->shared_from_this();
    return Status::OK();
}

Status RuntimeFilterMergeController::remove_entity(UniqueId queryId) {
    std::lock_guard<std::mutex> guard(_controller_mutex);
    LOG(WARNING) << "remove entity, query-id:" << queryId;
    _filter_controller_map.erase(queryId.to_string());
    return Status::OK();
}

// auto called while call ~std::shared_ptr<RuntimeFilterMergeControllerEntity>
void runtime_filter_merge_entity_close(RuntimeFilterMergeController* controller,
                                       RuntimeFilterMergeControllerEntity* entity) {
    controller->remove_entity(entity->query_id());
    delete entity;
}

} // namespace doris