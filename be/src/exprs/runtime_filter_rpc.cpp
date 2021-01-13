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

#include "common/status.h"
#include "exprs/runtime_filter.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

// for rpc
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "service/brpc.h"
#include "util/brpc_stub_cache.h"

namespace doris {

struct ShuffleRuntimeFilter::rpc_context {
    PMergeFilterRequest request;
    PMergeFilterResponse response;
    brpc::Controller cntl;
    brpc::CallId cid;
};

Status ShuffleRuntimeFilter::push_to_remote(RuntimeState* state, const TNetworkAddress* addr) {
    DCHECK(is_producer());
    DCHECK(_rpc_context == nullptr);
    PBackendService_Stub* stub = state->exec_env()->brpc_stub_cache()->get_stub(*addr);
    _rpc_context = std::make_shared<ShuffleRuntimeFilter::rpc_context>();
    void* data = nullptr;
    int len = 0;

    auto pquery_id = _rpc_context->request.mutable_query_id();
    pquery_id->set_hi(_state->query_id().hi);
    pquery_id->set_lo(_state->query_id().lo);

    _rpc_context->request.set_filter_id(_runtime_filter_desc->filter_id);

    RETURN_IF_ERROR(serialize(&_rpc_context->request, &data, &len));
    LOG(WARNING) << "Producer:" << _rpc_context->request.ShortDebugString();
    if (len > 0) {
        DCHECK(data != nullptr);
        _rpc_context->cntl.request_attachment().append(data, len);
    }

    stub->merge_filter(&_rpc_context->cntl, &_rpc_context->request, &_rpc_context->response,
                       brpc::DoNothing());
    return Status::OK();
}

Status ShuffleRuntimeFilter::join_rpc() {
    DCHECK(is_producer());
    producer_close();
    if (_rpc_context != nullptr) {
        brpc::Join(_rpc_context->cid);
        if (_rpc_context->cntl.Failed()) {
            LOG(WARNING) << "shuffle runtimefilter rpc err:" << _rpc_context->cntl.ErrorText();
        }
    }
    return Status::OK();
}
} // namespace doris