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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_RUNTIME_PREDICATE_H
#define DORIS_BE_SRC_QUERY_EXPRS_RUNTIME_PREDICATE_H

#include <condition_variable>
#include <list>
#include <map>
#include <mutex>

#include "gen_cpp/Exprs_types.h"
#include "runtime/types.h"
#include "util/uid_util.h"

namespace doris {
class Predicate;
class ObjectPool;
class ExprContext;
class RuntimeState;
class RuntimePredicateWrapper;
class MemTracker;
class TupleRow;
class PPublishFilterRequest;
class PMergeFilterRequest;
class TRuntimeFilterDesc;
class RowDescriptor;
class PMinMaxFilter;

enum class RuntimeFilterType {
    UNKNOWN_FILTER = -1,
    IN_FILTER = 0,
    MINMAX_FILTER = 1,
    BLOOM_FILTER = 2
};

struct RuntimeFilterParams {
    RuntimeFilterParams()
            : filter_type(RuntimeFilterType::UNKNOWN_FILTER),
              prob_expr_index(0),
              prob_expr_ctx(nullptr),
              hash_table_size(-1),
              bloom_filter_size(-1) {}
    RuntimeFilterParams(RuntimeFilterType type, int prob_index, ExprContext* prob_ctx,
                        int64_t table_size, int64_t filter_size = 0)
            : filter_type(type),
              prob_expr_index(prob_index),
              prob_expr_ctx(prob_ctx),
              hash_table_size(table_size),
              bloom_filter_size(filter_size) {}
    RuntimeFilterType filter_type;
    int prob_expr_index;
    ExprContext* prob_expr_ctx;
    // used in bloom filter
    int64_t hash_table_size;
    // used in bloom filter
    // if bloom_filter_size is setted ,hash_table_size will be ignore
    int64_t bloom_filter_size;
};

/// The runtimefilter is built in the join node.
/// The main purpose is to reduce the scanning amount of the
/// left table data according to the scanning results of the right table during the join process.
/// The runtimefilter will build some filter conditions.
/// that can be pushed down to node based on the results of the right table.
class RuntimeFilter {
public:
    RuntimeFilter(RuntimeState* state, MemTracker* mem_tracker, ObjectPool* pool);
    ~RuntimeFilter();
    // prob_index corresponds to the index of _probe_expr_ctxs in the join node
    // hash_table_size is the size of the hash_table
    Status create_runtime_predicate(const RuntimeFilterParams* params);

    // We need to know the data corresponding to a prob_index when building an expression
    void insert(int prob_index, void* data);

    // get pushdown expr_contexts
    // get_push_expr_ctxs could only called once
    Status get_push_expr_ctxs(std::list<ExprContext*>* push_expr_ctxs);

private:
    // A mapping from prob_index to [runtime_predicate_warppers]
    std::map<int, std::list<RuntimePredicateWrapper*>> _runtime_preds;
    RuntimeState* _state;
    MemTracker* _mem_tracker;
    ObjectPool* _pool;
};

struct UpdateRuntimeFilterParams {
    const PPublishFilterRequest* publish_request;
    const char* data;
};

struct MergeRuntimeFilterParams {
    const PMergeFilterRequest* merge_request;
    const char* data;
};

class ShuffleRuntimeFilter {
public:
    ShuffleRuntimeFilter(RuntimeState* state, MemTracker* tracker, ObjectPool* pool);
    ~ShuffleRuntimeFilter();
    bool is_ready() const { return _is_ready; }
    // only used for producer
    void insert(TupleRow* row);
    // only used for consumer
    // if filter is not ready for filter data scan_node
    // will wait util it ready or timeout
    bool await();
    void signal();

    bool is_producer() const { return _is_producer; }
    bool is_consumer() const { return !_is_producer; }
    void set_role(int role) { _is_producer = !role; }
    Status update_filter(const UpdateRuntimeFilterParams* param);
    Status merge_from(const ShuffleRuntimeFilter& shuffle_runtime_filter);

    Status init_with_desc(const TRuntimeFilterDesc* desc);
    Status init_from_params(const MergeRuntimeFilterParams* param);
    Status apply_init_update_params(const UpdateRuntimeFilterParams* param);
    RuntimeFilterType type() const { return _type; }

    Status get_push_expr_ctxs(std::list<ExprContext*>* push_expr_ctxs);
    Status get_push_expr_ctxs(std::vector<ExprContext*>* push_expr_ctxs, const RowDescriptor& desc,
                              const std::shared_ptr<MemTracker>& tracker);

    Status init_producer();

    Status serialize(PMergeFilterRequest* request, void** data, int* len);
    Status serialize(PPublishFilterRequest* request, void** data = nullptr, int* len = nullptr);

    Status get_data(void** data, int* len);

    // producer should call
    Status producer_prepare(const RowDescriptor& desc);

    Status producer_close();

    // consumer should call
    Status consumer_close();

    Status consumer_prepare(const RowDescriptor& desc);

    Status push_to_remote(RuntimeState* state, const TNetworkAddress* addr);

    Status join_rpc();

private:
    void to_protobuf(PMinMaxFilter* filter);
    template <class T>
    Status _serialize(T* request, void** data, int* len);
    // used for await or signal
    std::mutex _inner_mutex;
    std::condition_variable _inner_cv;
    bool _is_producer;
    bool _is_ready;
    RuntimeFilterType _type;
    RuntimePredicateWrapper* _wrapper;
    UniqueId _query_id;

    // will free by pool
    const TRuntimeFilterDesc* _runtime_filter_desc = nullptr;
    RuntimeState* _state;
    MemTracker* _mem_tracker;
    ObjectPool* _pool;

    ExprContext* _prob_ctx;
    ExprContext* _build_ctx;

    std::vector<ExprContext*> _target_ctxs;

    struct rpc_context;
    std::shared_ptr<rpc_context> _rpc_context;
};

} // namespace doris

#endif
