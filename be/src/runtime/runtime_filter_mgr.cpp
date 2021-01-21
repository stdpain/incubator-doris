#include "runtime/runtime_filter_mgr.h"

#include <string>

#include "client_cache.h"
#include "exprs/runtime_filter.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "service/brpc.h"
#include "util/brpc_stub_cache.h"
#include "util/time.h"

namespace doris {

template <class RPCRequest, class RPCResponse>
struct async_rpc_context {
    RPCRequest request;
    RPCResponse response;
    brpc::Controller cntl;
    brpc::CallId cid;
};

RuntimeFilterMgr::RuntimeFilterMgr(const UniqueId& query_id, RuntimeState* state) : _state(state) {}

RuntimeFilterMgr::~RuntimeFilterMgr() {}

Status RuntimeFilterMgr::init() {
    DCHECK(_state->instance_mem_tracker().get() != nullptr);
    _tracker = _state->instance_mem_tracker().get();
    return Status::OK();
}

Status RuntimeFilterMgr::get_filter_by_role(const int filter_id, const int role,
                                            IRuntimeFilter** target) {
    std::string key = std::to_string(filter_id);
    std::map<std::string, RuntimeFilterMgrVal>* filter_map = nullptr;

    if (role == ROLE_CONSUMER) {
        filter_map = &_consumer_map;
    } else {
        filter_map = &_producer_map;
    }

    auto iter = filter_map->find(key);
    if (iter == filter_map->end()) {
        LOG(WARNING) << "unknown filter...:" << key;
        return Status::InvalidArgument("unknown filter");
    }
    *target = iter->second.filter;
    return Status::OK();
}

Status RuntimeFilterMgr::get_consume_filter(const int filter_id, IRuntimeFilter** consumer_filter) {
    return get_filter_by_role(filter_id, ROLE_CONSUMER, consumer_filter);
}

Status RuntimeFilterMgr::get_producer_filter(const int filter_id,
                                             IRuntimeFilter** producer_filter) {
    return get_filter_by_role(filter_id, ROLE_PRODUCER, producer_filter);
}

Status RuntimeFilterMgr::regist_filter(const int role, const TRuntimeFilterDesc& desc,
                                       int node_id) {
    DCHECK((role == ROLE_CONSUMER && node_id >= 0));
    std::string key = std::to_string(desc.filter_id);

    std::map<std::string, RuntimeFilterMgrVal>* filter_map = nullptr;
    if (role == ROLE_CONSUMER) {
        filter_map = &_consumer_map;
    } else {
        filter_map = &_producer_map;
    }

    auto iter = filter_map->find(key);
    if (iter != filter_map->end()) {
        return Status::InvalidArgument("filter has registed");
    }

    RuntimeFilterMgrVal filter_mgr_val;
    filter_mgr_val.role = role;
    filter_mgr_val.runtime_filter_desc = &desc; // dangerous

    RETURN_IF_ERROR(IRuntimeFilter::create(_state, _tracker, &_pool, &desc, role, -1,
                                           &filter_mgr_val.filter));

    filter_mgr_val.filter->set_role(role);
    filter_mgr_val.filter->init_with_desc(&desc);
    filter_map->emplace(key, filter_mgr_val);

    return Status::OK();
}

Status RuntimeFilterMgr::update_filter(const PPublishFilterRequest* request, const char* data) {
    UpdateRuntimeFilterParams params;
    params.request = request;
    params.data = data;
    int filter_id = request->filter_id();
    IRuntimeFilter* real_filter = nullptr;
    RETURN_IF_ERROR(get_consume_filter(filter_id, &real_filter));
    return real_filter->update_filter(&params);
}

void RuntimeFilterMgr::set_runtime_filter_params(
        const TRuntimeFilterParams& runtime_filter_params) {
    this->merge_addr = runtime_filter_params.runtime_filter_merge_addr;
}

Status RuntimeFilterMgr::get_merge_addr(TNetworkAddress* addr) {
    *addr = this->merge_addr;
    return Status::OK();
}

Status RuntimeFilterMergeControllerEntity::_init_with_desc(
        const TRuntimeFilterDesc* runtime_filter_desc,
        const std::vector<doris::TRuntimeFilterTargetParams>* target_info) {
    std::lock_guard<std::mutex> guard(_filter_map_mutex);
    std::shared_ptr<RuntimeFilterCntlVal> cntVal = std::make_shared<RuntimeFilterCntlVal>();
    // runtime_filter_desc and target will be released,
    // so we need to copy to cntVal
    cntVal->runtime_filter_desc = *runtime_filter_desc;
    cntVal->target_info = *target_info;
    cntVal->pool.reset(new ObjectPool());
    cntVal->tracker = MemTracker::CreateTracker();
    if (runtime_filter_desc->is_broadcast_join) {
    }
    cntVal->filter = cntVal->pool->add(
            new ShuffleRuntimeFilter(nullptr, cntVal->tracker.get(), cntVal->pool.get()));

    std::string filter_id = std::to_string(runtime_filter_desc->filter_id);
    LOG(INFO) << "entity filter id:" << filter_id;
    cntVal->filter->init_with_desc(&cntVal->runtime_filter_desc);
    _filter_map.emplace(filter_id, cntVal);
    return Status::OK();
}

Status RuntimeFilterMergeControllerEntity::init(UniqueId query_id,
                                                const TRuntimeFilterParams& runtime_filter_params) {
    _query_id = query_id;
    for (auto& filterid_to_desc : runtime_filter_params.rid_to_runtime_filter) {
        int filter_id = filterid_to_desc.first;
        const auto& target_iter = runtime_filter_params.rid_to_target_param.find(filter_id);
        if (target_iter == runtime_filter_params.rid_to_target_param.end()) {
            return Status::InternalError("runtime filter params meet error");
        }
        _init_with_desc(&filterid_to_desc.second, &target_iter->second);
        // target_iter->second
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
        IRuntimeFilter* runtime_filter = nullptr;
        if (cntVal->filter->is_broadcast_join()) {
            runtime_filter = pool.add(new BoardCastRuntimeFilter(nullptr, tracker.get(), &pool));
        } else {
            runtime_filter = pool.add(new ShuffleRuntimeFilter(nullptr, tracker.get(), &pool));
        }
        RETURN_IF_ERROR(runtime_filter->init_with_proto_param(&params));
        RETURN_IF_ERROR(cntVal->filter->merge_from(runtime_filter));
        cntVal->arrive_id.insert(UniqueId(request->fragment_id()).to_string());
        merged_size = cntVal->arrive_id.size();
    }

    LOG(INFO) << "merge size:" << merged_size;
    if (merged_size == cntVal->target_info.size()) {
        // prepare rpc context
        using PPublishFilterRpcContext =
                async_rpc_context<PPublishFilterRequest, PPublishFilterResponse>;
        std::vector<std::unique_ptr<PPublishFilterRpcContext>> rpc_contexts;
        rpc_contexts.reserve(cntVal->target_info.size());

        butil::IOBuf request_attachment;

        PPublishFilterRequest apply_request;
        // serialize filter
        void* data = nullptr;
        int len = 0;
        bool has_attachment = false;
        RETURN_IF_ERROR(cntVal->filter->serialize(&apply_request, &data, &len));
        if (data != nullptr && len > 0) {
            request_attachment.append(data, len);
            has_attachment = true;
        }

        // async send publish rpc
        std::vector<TRuntimeFilterTargetParams>& targets = cntVal->target_info;
        for (size_t i = 0; i < targets.size(); i++) {
            rpc_contexts.emplace_back(new PPublishFilterRpcContext);
            rpc_contexts[i]->request = apply_request;
            rpc_contexts[i]->request.set_filter_id(request->filter_id());
            *rpc_contexts[i]->request.mutable_query_id() = request->query_id();
            if (has_attachment) {
                rpc_contexts[i]->cntl.request_attachment().append(request_attachment);
            }

            // set fragment-id
            auto request_fragment_id = rpc_contexts[i]->request.mutable_fragment_id();
            request_fragment_id->set_hi(targets[i].target_fragment_instance_id.hi);
            request_fragment_id->set_lo(targets[i].target_fragment_instance_id.lo);

            PBackendService_Stub* stub = ExecEnv::GetInstance()->brpc_stub_cache()->get_stub(
                    targets[i].target_fragment_instance_addr);
            LOG(INFO) << "send filter to:" << targets[i].target_fragment_instance_addr.hostname
                      << ",endpoint size:" << targets[i].target_fragment_instance_addr.port;
            if (stub == nullptr) {
                rpc_contexts.pop_back();
            }
            stub->apply_filter(&rpc_contexts[i]->cntl, &rpc_contexts[i]->request,
                               &rpc_contexts[i]->response, nullptr);
        }
        /// TODO: use async and join rpc
    }
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
    /// TODO(stdpain):
    LOG(WARNING) << "add entity, query-id:" << query_id_str;

    std::shared_ptr<RuntimeFilterMergeControllerEntity>& filter_merge_controller = *handle;

    const TRuntimeFilterParams& filter_params = params.params.runtime_filter_params;
    if (params.params.__isset.runtime_filter_params) {
        RETURN_IF_ERROR(filter_merge_controller->init(query_id, filter_params));
    }
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