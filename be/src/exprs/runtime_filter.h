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

#include "exprs/expr_context.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/types.h"
#include "util/runtime_profile.h"
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
    RuntimeFilterParams() : filter_type(RuntimeFilterType::UNKNOWN_FILTER), bloom_filter_size(-1) {}

    RuntimeFilterParams(RuntimeFilterType type, ExprContext* prob_ctx, int64_t filter_size = 0)
            : filter_type(type), bloom_filter_size(filter_size) {}

    RuntimeFilterType filter_type;
    PrimitiveType column_return_type;
    // used in bloom filter
    int64_t bloom_filter_size;
};

struct UpdateRuntimeFilterParams {
    const PPublishFilterRequest* request;
    const char* data;
};

struct MergeRuntimeFilterParams {
    const PMergeFilterRequest* request;
    const char* data;
};

/// The runtimefilter is built in the join node.
/// The main purpose is to reduce the scanning amount of the
/// left table data according to the scanning results of the right table during the join process.
/// The runtimefilter will build some filter conditions.
/// that can be pushed down to node based on the results of the right table.
class IRuntimeFilter {
public:
    IRuntimeFilter(RuntimeState* state, MemTracker* mem_tracker, ObjectPool* pool)
            : _state(state),
              _mem_tracker(mem_tracker),
              _pool(pool),
              _runtime_filter_type(RuntimeFilterType::UNKNOWN_FILTER),
              _filter_id(-1),
              _is_broadcast_join(true),
              _has_remote_target(false),
              _has_local_target(false),
              _is_ready(false),
              _role(true),
              _expr_order(-1),
              _always_true(false),
              _probe_ctx(nullptr) {}

    virtual ~IRuntimeFilter() = default;

    static Status create(RuntimeState* state, MemTracker* tracker, ObjectPool* pool,
                         const TRuntimeFilterDesc* desc, int role, int node_id,
                         IRuntimeFilter** res);

    // insert data to build filter
    // only used for producer
    virtual void insert(void* data);

    // prepare something before insert
    virtual void prepare() {};

    // publish filter
    // push filter to remote node or push down it to scan_node
    virtual void publish() {};

    RuntimeFilterType type() const { return _runtime_filter_type; }

    // get push down expr context
    // This function can only be called once
    // _wrapper's function will be clear
    // only consumer could call this
    Status get_push_expr_ctxs(std::list<ExprContext*>* push_expr_ctxs);

    // This function can only be called once
    // only produer could call this
    Status get_push_expr_ctxs(std::list<ExprContext*>* push_expr_ctxs, ExprContext* probe_ctx);

    // This function can be called multiple times
    Status get_prepared_context(std::vector<ExprContext*>* push_expr_ctxs,
                                const RowDescriptor& desc,
                                const std::shared_ptr<MemTracker>& tracker);

    bool is_broadcast_join() const { return _is_broadcast_join; }

    bool is_ready() const { return _is_ready; }

    bool is_producer() const { return _role; }
    bool is_consumer() const { return !_role; }
    void set_role(int consumer) { _role = !consumer; }
    int expr_order() { return _expr_order; }

    // only used for consumer
    // if filter is not ready for filter data scan_node
    // will wait util it ready or timeout
    // This function will wait at most config::runtime_filter_shuffle_wait_time_ms
    bool await();
    // this function will be called if a runtime filter sent by rpc
    // it will nodify all wait threads
    void signal();

    // init filter with desc
    virtual Status init_with_desc(const TRuntimeFilterDesc* desc, int node_id = -1);

    // serialize _wrapper to protobuf
    Status serialize(PMergeFilterRequest* request, void** data, int* len);
    Status serialize(PPublishFilterRequest* request, void** data = nullptr, int* len = nullptr);

    Status merge_from(const IRuntimeFilter* shuffle_runtime_filter);

    Status init_with_proto_param(const MergeRuntimeFilterParams* param);
    Status init_with_proto_param(const UpdateRuntimeFilterParams* param);

    virtual Status update_filter(const UpdateRuntimeFilterParams* param) = 0;

    // consumer should call before released
    virtual Status consumer_close() = 0;

protected:
    // serialize _wrapper to protobuf
    void to_protobuf(PMinMaxFilter* filter);

    template <class T>
    Status _serialize(T* request, void** data, int* len);

    template <class T>
    Status _init_with_proto_param(const T* param);

protected:
    RuntimeState* _state;
    MemTracker* _mem_tracker;
    ObjectPool* _pool;
    // _wrapper is a runtime filter function wrapper
    // _wrapper should alloc from _pool
    RuntimePredicateWrapper* _wrapper;
    // runtime filter type
    RuntimeFilterType _runtime_filter_type;
    // runtime filter id
    int _filter_id;
    // Specific types BoardCast or Shuffle
    bool _is_broadcast_join;
    // will apply to remote node
    bool _has_remote_target;
    // will apply to local node
    bool _has_local_target;
    // filter is ready for consumer
    bool _is_ready;
    // role consumer or producer
    bool _role;
    // expr index
    int _expr_order;
    // used for await or signal
    std::mutex _inner_mutex;
    std::condition_variable _inner_cv;

    // if set always_true = true
    // this filter won't filter any data
    bool _always_true;

    // build expr_context
    // ExprContext* _build_ctx;
    // probe expr_context
    // it only used in consumer to generate runtime_filter expr_context
    // we don't have to prepare it or close it
    ExprContext* _probe_ctx;

    // some runtime filter will generate
    // multiple contexts such as minmax filter
    // these context is called prepared by this,
    // consumer_close should be called before release
    std::vector<ExprContext*> _push_down_ctxs;
};

/// LocalRuntimeFilter is only used in boardcast join
class BoardCastRuntimeFilter : public IRuntimeFilter {
public:
    BoardCastRuntimeFilter(RuntimeState* state, MemTracker* mem_tracker, ObjectPool* pool);
    ~BoardCastRuntimeFilter();

    // prob_index corresponds to the index of _probe_expr_ctxs in the join node
    // hash_table_size is the size of the hash_table
    // Status create_runtime_predicate(const RuntimeFilterParams* params);

    virtual Status init_with_desc(const TRuntimeFilterDesc* desc, int node_id);

    virtual Status update_filter(const UpdateRuntimeFilterParams* param);

    virtual Status consumer_close();
};

/// The shuffle runtime filter is built from the join node.
class ShuffleRuntimeFilter : public IRuntimeFilter {
public:
    ShuffleRuntimeFilter(RuntimeState* state, MemTracker* tracker, ObjectPool* pool);
    ~ShuffleRuntimeFilter();
    bool is_ready() const { return _is_ready; }

    // update a filter from params and `signal` will be called
    Status update_filter(const UpdateRuntimeFilterParams* param) override;

    // One of the next three functions must be called
    Status init_with_desc(const TRuntimeFilterDesc* desc, int node_id) override;

    // consumer should call
    Status consumer_close() override;

    // async push runtimefilter to remote node
    Status push_to_remote(RuntimeState* state, const TNetworkAddress* addr);
    Status join_rpc();

private:
    UniqueId _query_id;

    struct rpc_context;
    std::shared_ptr<rpc_context> _rpc_context;
};

/// this class used in a hash join node
/// Provide a unified interface for other classes
class RuntimeFilterSlots {
public:
    RuntimeFilterSlots(const std::vector<ExprContext*>& prob_expr_ctxs,
                       const std::vector<ExprContext*>& build_expr_ctxs,
                       const std::vector<TRuntimeFilterDesc>& runtime_filter_descs)
            : _probe_expr_context(prob_expr_ctxs),
              _build_expr_context(build_expr_ctxs),
              _runtime_filter_descs(runtime_filter_descs) {}

    Status init(RuntimeState* state, ObjectPool* pool, MemTracker* tracker);

    void insert(TupleRow* row) {
        for (int i = 0; i < _build_expr_context.size(); ++i) {
            auto iter = _runtime_filters.find(i);
            if (iter != _runtime_filters.end()) {
                void* val = _build_expr_context[i]->get_value(row);
                for (auto filter : iter->second) {
                    filter->insert(val);
                }
            }
        }
    }

    // publish runtime filter
    // Status publish();


private:
    const std::vector<ExprContext*>& _probe_expr_context;
    const std::vector<ExprContext*>& _build_expr_context;
    const std::vector<TRuntimeFilterDesc>& _runtime_filter_descs;
    // prob_contition index -> [IRuntimeFilter]
    std::map<int, std::list<IRuntimeFilter*>> _runtime_filters;
};

} // namespace doris

#endif
