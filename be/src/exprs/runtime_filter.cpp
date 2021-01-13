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

#include "runtime_filter.h"

#include <memory>

#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/binary_predicate.h"
#include "exprs/bloomfilter_predicate.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/hybrid_set.h"
#include "exprs/in_predicate.h"
#include "exprs/literal.h"
#include "exprs/predicate.h"
#include "gen_cpp/internal_service.pb.h"
#include "gen_cpp/types.pb.h"
#include "runtime/runtime_state.h"
#include "runtime/type_limit.h"

namespace doris {
// only used in Runtime Filter
class MinMaxFuncBase {
public:
    virtual void insert(void* data) = 0;
    virtual bool find(void* data) = 0;
    virtual bool is_empty() = 0;
    virtual void* get_max() = 0;
    virtual void* get_min() = 0;
    // assign minmax data
    virtual Status assign(void* min_data, void* max_data) = 0;
    // merge from other minmax_func
    virtual Status merge(MinMaxFuncBase* minmax_func) = 0;
    // create min-max filter function
    static MinMaxFuncBase* create_minmax_filter(PrimitiveType type);
};

template <class T>
class MinMaxNumFunc : public MinMaxFuncBase {
public:
    MinMaxNumFunc() = default;
    ~MinMaxNumFunc() = default;
    virtual void insert(void* data) {
        if (data == nullptr) return;
        T val_data = *reinterpret_cast<T*>(data);
        if (_empty) {
            _min = val_data;
            _max = val_data;
            _empty = false;
            return;
        }
        if (val_data < _min) {
            _min = val_data;
        } else if (val_data > _max) {
            _max = val_data;
        }
    }

    virtual bool find(void* data) {
        if (data == nullptr) {
            return false;
        }
        T val_data = *reinterpret_cast<T*>(data);
        return val_data >= _min && val_data <= _max;
    }

    Status merge(MinMaxFuncBase* minmax_func) {
        MinMaxNumFunc<T>* other_minmax = static_cast<MinMaxNumFunc<T>*>(minmax_func);
        if (other_minmax->_min < _min) {
            _min = other_minmax->_min;
        }
        if (other_minmax->_max > _max) {
            _max = other_minmax->_max;
        }
        _empty = true;
        return Status::OK();
    }

    virtual bool is_empty() { return _empty; }

    virtual void* get_max() { return &_max; }

    virtual void* get_min() { return &_min; }

    virtual Status assign(void* min_data, void* max_data) {
        _min = *(T*)min_data;
        _max = *(T*)max_data;
        return Status::OK();
    }

private:
    T _max = type_limit<T>::min();
    T _min = type_limit<T>::max();
    // we use _empty to avoid compare twice
    bool _empty = true;
};

MinMaxFuncBase* MinMaxFuncBase::create_minmax_filter(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return new (std::nothrow) MinMaxNumFunc<bool>();

    case TYPE_TINYINT:
        return new (std::nothrow) MinMaxNumFunc<int8_t>();

    case TYPE_SMALLINT:
        return new (std::nothrow) MinMaxNumFunc<int16_t>();

    case TYPE_INT:
        return new (std::nothrow) MinMaxNumFunc<int32_t>();

    case TYPE_BIGINT:
        return new (std::nothrow) MinMaxNumFunc<int64_t>();

    case TYPE_FLOAT:
        return new (std::nothrow) MinMaxNumFunc<float>();

    case TYPE_DOUBLE:
        return new (std::nothrow) MinMaxNumFunc<double>();

    case TYPE_DATE:
    case TYPE_DATETIME:
        return new (std::nothrow) MinMaxNumFunc<DateTimeValue>();

    case TYPE_DECIMAL:
        return new (std::nothrow) MinMaxNumFunc<DecimalValue>();

    case TYPE_DECIMALV2:
        return new (std::nothrow) MinMaxNumFunc<DecimalV2Value>();

    case TYPE_LARGEINT:
        return new (std::nothrow) MinMaxNumFunc<__int128>();

    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return new (std::nothrow) MinMaxNumFunc<StringValue>();
    default:
        DCHECK(false) << "Invalid type.";
    }
    return NULL;
}

// PrimitiveType->TExprNodeType
static TExprNodeType::type get_expr_node_type(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return TExprNodeType::BOOL_LITERAL;

    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        return TExprNodeType::INT_LITERAL;

    case TYPE_LARGEINT:
        return TExprNodeType::LARGE_INT_LITERAL;
        break;

    case TYPE_NULL:
        return TExprNodeType::NULL_LITERAL;

    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_TIME:
        return TExprNodeType::FLOAT_LITERAL;
        break;

    case TYPE_DECIMAL:
    case TYPE_DECIMALV2:
        return TExprNodeType::DECIMAL_LITERAL;

    case TYPE_DATETIME:
        return TExprNodeType::DATE_LITERAL;

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
        return TExprNodeType::STRING_LITERAL;

    default:
        DCHECK(false) << "Invalid type.";
        return TExprNodeType::NULL_LITERAL;
    }
}

PColumnType to_proto(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
        return PColumnType::COLUMN_TYPE_INT;
    case TYPE_BIGINT:
        return PColumnType::COLUMN_TYPE_LONG;

    case TYPE_LARGEINT:
        break;

    case TYPE_NULL:

    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_TIME:

    case TYPE_DECIMAL:
    case TYPE_DECIMALV2:

    case TYPE_DATETIME:

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:

    default:
        DCHECK(false) << "Invalid type.";
    }
    DCHECK(false);
    return PColumnType::COLUMN_TYPE_INT;
}

PrimitiveType to_primitive_type(PColumnType type) {
    switch (type) {
    case PColumnType::COLUMN_TYPE_INT:
        return TYPE_INT;
    case PColumnType::COLUMN_TYPE_LONG:
        return TYPE_BIGINT;
    default:
        DCHECK(false);
    }
    return TYPE_INT;
}

static TTypeDesc create_type_desc(PrimitiveType type) {
    TTypeDesc type_desc;
    std::vector<TTypeNode> node_type;
    node_type.emplace_back();
    TScalarType scalarType;
    scalarType.__set_type(to_thrift(type));
    scalarType.__set_len(-1);
    node_type.back().__set_scalar_type(scalarType);
    type_desc.__set_types(node_type);
    return type_desc;
}

// only used to push down to olap engine
Expr* create_literal(ObjectPool* pool, PrimitiveType type, const void* data) {
    TExprNode node;

    switch (type) {
    case TYPE_BOOLEAN: {
        TBoolLiteral boolLiteral;
        boolLiteral.__set_value(*reinterpret_cast<const bool*>(data));
        node.__set_bool_literal(boolLiteral);
        break;
    }
    case TYPE_TINYINT: {
        TIntLiteral intLiteral;
        intLiteral.__set_value(*reinterpret_cast<const int8_t*>(data));
        node.__set_int_literal(intLiteral);
        break;
    }
    case TYPE_SMALLINT: {
        TIntLiteral intLiteral;
        intLiteral.__set_value(*reinterpret_cast<const int16_t*>(data));
        node.__set_int_literal(intLiteral);
        break;
    }
    case TYPE_INT: {
        TIntLiteral intLiteral;
        intLiteral.__set_value(*reinterpret_cast<const int32_t*>(data));
        node.__set_int_literal(intLiteral);
        break;
    }
    case TYPE_BIGINT: {
        TIntLiteral intLiteral;
        intLiteral.__set_value(*reinterpret_cast<const int64_t*>(data));
        node.__set_int_literal(intLiteral);
        break;
    }
    case TYPE_LARGEINT: {
        TLargeIntLiteral largeIntLiteral;
        largeIntLiteral.__set_value(
                LargeIntValue::to_string(*reinterpret_cast<const int128_t*>(data)));
        node.__set_large_int_literal(largeIntLiteral);
        break;
    }
    case TYPE_FLOAT: {
        TFloatLiteral floatLiteral;
        floatLiteral.__set_value(*reinterpret_cast<const float*>(data));
        node.__set_float_literal(floatLiteral);
        break;
    }
    case TYPE_DOUBLE: {
        TFloatLiteral floatLiteral;
        floatLiteral.__set_value(*reinterpret_cast<const double*>(data));
        node.__set_float_literal(floatLiteral);
        break;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        TDateLiteral dateLiteral;
        char convert_buffer[30];
        reinterpret_cast<const DateTimeValue*>(data)->to_string(convert_buffer);
        dateLiteral.__set_value(convert_buffer);
        node.__set_date_literal(dateLiteral);
        break;
    }
    case TYPE_DECIMAL: {
        TDecimalLiteral decimalLiteral;
        decimalLiteral.__set_value(reinterpret_cast<const DecimalValue*>(data)->to_string());
        node.__set_decimal_literal(decimalLiteral);
        break;
    }
    case TYPE_DECIMALV2: {
        TDecimalLiteral decimalLiteral;
        decimalLiteral.__set_value(reinterpret_cast<const DecimalV2Value*>(data)->to_string());
        node.__set_decimal_literal(decimalLiteral);
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        const StringValue* string_value = reinterpret_cast<const StringValue*>(data);
        TStringLiteral tstringLiteral;
        tstringLiteral.__set_value(std::string(string_value->ptr, string_value->len));
        node.__set_string_literal(tstringLiteral);
        break;
    }
    default:
        DCHECK(false);
        return NULL;
    }
    node.__set_node_type(get_expr_node_type(type));
    node.__set_type(create_type_desc(type));
    return pool->add(new Literal(node));
}

BinaryPredicate* create_bin_predicate(PrimitiveType prim_type, TExprOpcode::type opcode) {
    TExprNode node;
    TScalarType tscalar_type;
    tscalar_type.__set_type(TPrimitiveType::BOOLEAN);
    TTypeNode ttype_node;
    ttype_node.__set_type(TTypeNodeType::SCALAR);
    ttype_node.__set_scalar_type(tscalar_type);
    TTypeDesc t_type_desc;
    t_type_desc.types.push_back(ttype_node);
    node.__set_type(t_type_desc);
    node.__set_opcode(opcode);
    node.__set_child_type(to_thrift(prim_type));
    node.__set_num_children(2);
    node.__set_output_scale(-1);
    node.__set_node_type(TExprNodeType::BINARY_PRED);
    return (BinaryPredicate*)BinaryPredicate::from_thrift(node);
}

// This class will help us build really pushdown expressions
//
class RuntimePredicateWrapper {
public:
    RuntimePredicateWrapper(RuntimeState* state, MemTracker* tracker, ObjectPool* pool,
                            const RuntimeFilterParams* params)
            : _state(state),
              _tracker(tracker),
              _pool(pool),
              _filter_type(params->filter_type),
              _expr_ctx(params->prob_expr_ctx) {}
    // for a 'tmp' runtime predicate wrapper
    // only could called assign method or as a param for merge
    RuntimePredicateWrapper(MemTracker* tracker, ObjectPool* pool, RuntimeFilterType type)
            : _state(nullptr),
              _tracker(tracker),
              _pool(pool),
              _filter_type(type),
              _expr_ctx(nullptr) {}
    // init runtimefilter wrapper
    // alloc memory to init runtime filter function
    Status init(const RuntimeFilterParams* params) {
        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            _hybrid_set.reset(HybridSetBase::create_set(_expr_ctx->root()->type().type));
            break;
        }
        case RuntimeFilterType::MINMAX_FILTER: {
            _minmax_func.reset(
                    MinMaxFuncBase::create_minmax_filter(_expr_ctx->root()->type().type));
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            _bloomfilter_func.reset(BloomFilterFuncBase::create_bloom_filter(
                    _tracker, _expr_ctx->root()->type().type));
            Status bloom_filter_status;
            if (params->bloom_filter_size > 0) {
                bloom_filter_status =
                        _bloomfilter_func->init_with_fixed_length(params->bloom_filter_size);
            } else {
                bloom_filter_status = _bloomfilter_func->init(params->hash_table_size);
            }
            if (!bloom_filter_status.ok()) {
                _bloomfilter_func.reset();
                return bloom_filter_status;
            }
            break;
        }
        default:
            break;
        }
        return Status::OK();
    }

    void insert(void* data) {
        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            _hybrid_set->insert(data);
            break;
        }
        case RuntimeFilterType::MINMAX_FILTER: {
            _minmax_func->insert(data);
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            DCHECK(_bloomfilter_func != nullptr);
            _bloomfilter_func->insert(data);
            break;
        }
        default:
            DCHECK(false);
            break;
        }
    }

    template <class T>
    Status get_push_context(T* container) {
        DCHECK(container != nullptr);
        DCHECK(_state != nullptr);
        DCHECK(_pool != nullptr);
        PrimitiveType expr_primitive_type = _expr_ctx->root()->type().type;

        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            TTypeDesc type_desc = create_type_desc(_expr_ctx->root()->type().type);
            TExprNode node;
            node.__set_type(type_desc);
            node.__set_node_type(TExprNodeType::IN_PRED);
            node.in_predicate.__set_is_not_in(false);
            node.__set_opcode(TExprOpcode::FILTER_IN);
            node.__isset.vector_opcode = true;
            node.__set_vector_opcode(to_in_opcode(expr_primitive_type));
            auto in_pred = _pool->add(new InPredicate(node));
            RETURN_IF_ERROR(in_pred->prepare(_state, _hybrid_set.release()));
            in_pred->add_child(Expr::copy(_pool, _expr_ctx->root()));
            ExprContext* ctx = _pool->add(new ExprContext(in_pred));
            container->push_back(ctx);
            break;
        }
        case RuntimeFilterType::MINMAX_FILTER: {
            // create max filter
            auto max_pred = create_bin_predicate(expr_primitive_type, TExprOpcode::LE);
            auto max_literal = create_literal(_pool, expr_primitive_type, _minmax_func->get_max());
            max_pred->add_child(Expr::copy(_pool, _expr_ctx->root()));
            max_pred->add_child(max_literal);
            container->push_back(_pool->add(new ExprContext(max_pred)));
            // create min filter
            auto min_pred = create_bin_predicate(expr_primitive_type, TExprOpcode::GE);
            auto min_literal = create_literal(_pool, expr_primitive_type, _minmax_func->get_min());
            min_pred->add_child(Expr::copy(_pool, _expr_ctx->root()));
            min_pred->add_child(min_literal);
            container->push_back(_pool->add(new ExprContext(min_pred)));
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            // create a bloom filter
            TTypeDesc type_desc = create_type_desc(_expr_ctx->root()->type().type);
            TExprNode node;
            node.__set_type(type_desc);
            node.__set_node_type(TExprNodeType::BLOOM_PRED);
            node.__set_opcode(TExprOpcode::RT_FILTER);
            node.__isset.vector_opcode = true;
            node.__set_vector_opcode(to_in_opcode(expr_primitive_type));
            auto bloom_pred = _pool->add(new BloomFilterPredicate(node));
            RETURN_IF_ERROR(bloom_pred->prepare(_state, _bloomfilter_func.release()));
            bloom_pred->add_child(Expr::copy(_pool, _expr_ctx->root()));
            ExprContext* ctx = _pool->add(new ExprContext(bloom_pred));
            container->push_back(ctx);
            break;
        }
        default:
            DCHECK(false);
            break;
        }
        return Status::OK();
    }

    Status merge(const RuntimePredicateWrapper* wrapper) {
        switch (_filter_type) {
        case RuntimeFilterType::IN_FILTER: {
            DCHECK(false) << "in filter should't apply in shuffle join";
            return Status::InternalError("in filter should't apply in shuffle join");
        }
        case RuntimeFilterType::MINMAX_FILTER: {
            _minmax_func->merge(wrapper->_minmax_func.get());
            break;
        }
        case RuntimeFilterType::BLOOM_FILTER: {
            if (_bloomfilter_func != nullptr) {
                _bloomfilter_func->merge(wrapper->_bloomfilter_func.get());
            }
            break;
        }
        default:
            DCHECK(false);
            return Status::InternalError("unknown runtime filter");
        }
        return Status::OK();
    }

    // used by shuffle runtime filter
    Status assign(const PBloomFilter* bloom_filter, const char* data) {
        DCHECK(_tracker != nullptr);
        // we won't use this class to insert or find any data
        // so any type is ok
        _bloomfilter_func.reset(
                BloomFilterFuncBase::create_bloom_filter(_tracker, PrimitiveType::TYPE_INT));
        return _bloomfilter_func->assign(data, bloom_filter->filter_length());
    }

    Status assign(const PMinMaxFilter* minmax_filter) {
        DCHECK(_tracker != nullptr);
        PrimitiveType type = to_primitive_type(minmax_filter->column_type());
        _minmax_func.reset(MinMaxFuncBase::create_minmax_filter(type));
        switch (type) {
        case TYPE_INT: {
            auto min_val = _pool->add(new int32_t);
            auto max_val = _pool->add(new int32_t);
            *min_val = minmax_filter->min_val().intval();
            *max_val = minmax_filter->max_val().intval();
            return _minmax_func->assign(min_val, max_val);
        }
        case TYPE_BIGINT: {
            auto min_val = _pool->add(new int64);
            auto max_val = _pool->add(new int64);
            *min_val = minmax_filter->min_val().longval();
            *max_val = minmax_filter->max_val().longval();
            return _minmax_func->assign(min_val, max_val);
        }
        default:
            break;
        }
        // get type from bloom filter
        // return _minmax_func->assign()
        return Status::InvalidArgument("not support!");
    }

    Status get_bloom_filter_desc(char** data, int* filter_length) {
        return _bloomfilter_func->get_data(data, filter_length);
    }

    Status get_minmax_filter_desc(void** min_data, void** max_data) {
        *min_data = _minmax_func->get_min();
        *max_data = _minmax_func->get_max();
        return Status::OK();
    }

    PrimitiveType expr_primitive_type() { return _expr_ctx->root()->type().type; }

private:
    RuntimeState* _state;
    MemTracker* _tracker;
    ObjectPool* _pool;
    RuntimeFilterType _filter_type;
    std::unique_ptr<MinMaxFuncBase> _minmax_func;
    std::unique_ptr<HybridSetBase> _hybrid_set;
    std::unique_ptr<BloomFilterFuncBase> _bloomfilter_func;
    ExprContext* _expr_ctx;
};

RuntimeFilter::RuntimeFilter(RuntimeState* state, MemTracker* mem_tracker, ObjectPool* pool)
        : _state(state), _mem_tracker(mem_tracker), _pool(pool) {}

RuntimeFilter::~RuntimeFilter() {}

Status RuntimeFilter::create_runtime_predicate(const RuntimeFilterParams* params) {
    switch (params->filter_type) {
    case RuntimeFilterType::IN_FILTER:
    case RuntimeFilterType::MINMAX_FILTER:
    case RuntimeFilterType::BLOOM_FILTER: {
        RuntimePredicateWrapper* wrapper =
                _pool->add(new RuntimePredicateWrapper(_state, _mem_tracker, _pool, params));
        // if wrapper init error
        // we will ignore this filter
        RETURN_IF_ERROR(wrapper->init(params));
        _runtime_preds[params->prob_expr_index].push_back(wrapper);
        break;
    }
    default:
        DCHECK(false) << "Invalid type.";
        return Status::NotSupported("not support filter type");
    }
    return Status::OK();
}

void RuntimeFilter::insert(int prob_index, void* data) {
    auto iter = _runtime_preds.find(prob_index);
    if (iter != _runtime_preds.end()) {
        for (auto filter : iter->second) {
            filter->insert(data);
        }
    }
}

Status RuntimeFilter::get_push_expr_ctxs(std::list<ExprContext*>* push_expr_ctxs) {
    DCHECK(push_expr_ctxs != nullptr);
    for (auto& iter : _runtime_preds) {
        for (auto pred : iter.second) {
            pred->get_push_context(push_expr_ctxs);
        }
    }
    return Status::OK();
}

ShuffleRuntimeFilter::ShuffleRuntimeFilter(RuntimeState* state, MemTracker* tracker,
                                           ObjectPool* pool)
        : _is_ready(false), _query_id(0, 0), _state(state), _mem_tracker(tracker), _pool(pool) {}

void ShuffleRuntimeFilter::insert(TupleRow* row) {
    DCHECK(is_producer());
    DCHECK(_build_ctx != nullptr);
    void* data = _build_ctx->get_value(row);
    _wrapper->insert(data);
}

bool ShuffleRuntimeFilter::await() {
    DCHECK(is_consumer());
    int64_t wait_times_ms = 100000;
    if (!_is_ready) {
        std::unique_lock<std::mutex> lock(_inner_mutex);
        return _inner_cv.wait_for(lock, std::chrono::milliseconds(wait_times_ms),
                                  [this] { return this->_is_ready; });
    }
    return true;
}

void ShuffleRuntimeFilter::signal() {
    DCHECK(is_consumer());
    _is_ready = true;
    _inner_cv.notify_all();
}

Status ShuffleRuntimeFilter::merge_from(const ShuffleRuntimeFilter& shuffle_runtime_filter) {
    if (shuffle_runtime_filter.type() != type()) {
        return Status::InvalidArgument("filter type error");
    }
    return _wrapper->merge(shuffle_runtime_filter._wrapper);
}

Status ShuffleRuntimeFilter::update_filter(const UpdateRuntimeFilterParams* param) {
    std::shared_ptr<MemTracker> tracker = MemTracker::CreateTracker();
    ObjectPool pool;
    ShuffleRuntimeFilter filter(nullptr, tracker.get(), &pool);
    RETURN_IF_ERROR(filter.apply_init_update_params(param));
    RETURN_IF_ERROR(_wrapper->merge(filter._wrapper));
    this->signal();
    return Status::OK();
}

Status ShuffleRuntimeFilter::init_with_desc(const TRuntimeFilterDesc* desc) {
    _runtime_filter_desc = desc;
    if (desc->type == TRuntimeFilterType::BLOOM) {
        _type = RuntimeFilterType::BLOOM_FILTER;
    } else if (desc->type == TRuntimeFilterType::MIN_MAX) {
        _type = RuntimeFilterType::MINMAX_FILTER;
    } else {
        return Status::InvalidArgument("unknown filter type");
    }

    auto first_id_expr = _runtime_filter_desc->planid_to_target_expr.begin();
    if (first_id_expr == _runtime_filter_desc->planid_to_target_expr.end()) {
        return Status::InvalidArgument("runtime filter desc empty");
    }

    auto sed = first_id_expr->second;
    ExprContext* target_ctx = nullptr;
    RETURN_IF_ERROR(Expr::create_expr_tree(_pool, sed, &target_ctx));

    RuntimeFilterParams params;
    params.filter_type = _type;
    params.prob_expr_ctx = target_ctx;
    params.bloom_filter_size = desc->bloom_filter_size_bytes;
    _wrapper = _pool->add(new RuntimePredicateWrapper(_state, _mem_tracker, _pool, &params));
    return _wrapper->init(&params);
}

RuntimeFilterType get_type(int filter_type) {
    switch (filter_type) {
    case PFilterType::BLOOM_FILTER: {
        return RuntimeFilterType::BLOOM_FILTER;
    }
    case PFilterType::MINMAX_FILTER:
        return RuntimeFilterType::MINMAX_FILTER;
    default:
        return RuntimeFilterType::UNKNOWN_FILTER;
    }
}

PFilterType get_type(RuntimeFilterType type) {
    switch (type) {
    case RuntimeFilterType::BLOOM_FILTER:
        return PFilterType::BLOOM_FILTER;
    case RuntimeFilterType::MINMAX_FILTER:
        return PFilterType::MINMAX_FILTER;
    default:
        return PFilterType::UNKNOW_FILTER;
    }
}

Status ShuffleRuntimeFilter::init_from_params(const MergeRuntimeFilterParams* param) {
    int filter_type = param->merge_request->filter_type();
    _wrapper = _pool->add(new RuntimePredicateWrapper(_mem_tracker, _pool, get_type(filter_type)));

    switch (filter_type) {
    case PFilterType::BLOOM_FILTER: {
        _type = RuntimeFilterType::BLOOM_FILTER;
        DCHECK(param->merge_request->has_bloom_filter());
        return _wrapper->assign(&param->merge_request->bloom_filter(), param->data);
    }
    case PFilterType::MINMAX_FILTER: {
        _type = RuntimeFilterType::MINMAX_FILTER;
        DCHECK(param->merge_request->has_minmax_filter());
        return _wrapper->assign(&param->merge_request->minmax_filter());
    }
    default:
        return Status::InvalidArgument("unknow filter type");
    }
    return Status::OK();
}

Status ShuffleRuntimeFilter::apply_init_update_params(const UpdateRuntimeFilterParams* param) {
    int filter_type = param->publish_request->filter_type();
    _wrapper = _pool->add(new RuntimePredicateWrapper(_mem_tracker, _pool, get_type(filter_type)));

    switch (filter_type) {
    case PFilterType::BLOOM_FILTER: {
        DCHECK(param->publish_request->has_bloom_filter());
        return _wrapper->assign(&param->publish_request->bloom_filter(), param->data);
    }
    case PFilterType::MINMAX_FILTER:
        DCHECK(param->publish_request->has_minmax_filter());
        return _wrapper->assign(&param->publish_request->minmax_filter());
    default:
        return Status::InvalidArgument("unknow filter type");
    }
    return Status::OK();
}

Status ShuffleRuntimeFilter::init_producer() {
    DCHECK(is_producer());
    // init build-context
    RETURN_IF_ERROR(Expr::create_expr_tree(_pool, _runtime_filter_desc->src_expr, &_build_ctx));
    return Status::OK();
}

Status ShuffleRuntimeFilter::producer_prepare(const RowDescriptor& desc) {
    RETURN_IF_ERROR(_build_ctx->prepare(_state, desc, _mem_tracker->shared_from_this()));
    RETURN_IF_ERROR(_build_ctx->open(_state));
    return Status::OK();
}

Status ShuffleRuntimeFilter::producer_close() {
    _build_ctx->close(_state);
    return Status::OK();
}

Status ShuffleRuntimeFilter::get_push_expr_ctxs(std::list<ExprContext*>* push_expr_ctxs) {
    return _wrapper->get_push_context(push_expr_ctxs);
}

Status ShuffleRuntimeFilter::get_push_expr_ctxs(std::vector<ExprContext*>* push_expr_ctxs,
                                                const RowDescriptor& desc,
                                                const std::shared_ptr<MemTracker>& tracker) {
    DCHECK(_is_ready);
    DCHECK(is_consumer());
    std::lock_guard<std::mutex> guard(_inner_mutex);
    if (!_target_ctxs.empty()) {
        push_expr_ctxs->insert(push_expr_ctxs->end(), _target_ctxs.begin(), _target_ctxs.end());
        return Status::OK();
    }
    RETURN_IF_ERROR(_wrapper->get_push_context(&_target_ctxs));
    Expr::prepare(_target_ctxs, _state, desc, tracker);
    Expr::open(_target_ctxs, _state);
    return Status::OK();
}

void ShuffleRuntimeFilter::to_protobuf(PMinMaxFilter* filter) {
    switch (_wrapper->expr_primitive_type()) {
    case TYPE_BOOLEAN: {
        break;
    }
    case TYPE_TINYINT: {
        break;
    }
    case TYPE_SMALLINT: {
        break;
    }
    case TYPE_INT: {
        filter->set_column_type(PColumnType::COLUMN_TYPE_INT);
        void* min_data = nullptr;
        void* max_data = nullptr;
        _wrapper->get_minmax_filter_desc(&min_data, &max_data);
        DCHECK(min_data != nullptr);
        DCHECK(max_data != nullptr);
        filter->mutable_min_val()->set_intval(*reinterpret_cast<const int32_t*>(min_data));
        filter->mutable_max_val()->set_intval(*reinterpret_cast<const int32_t*>(max_data));
        return;
    }
    case TYPE_BIGINT: {
        filter->set_column_type(PColumnType::COLUMN_TYPE_BIGINT);
        void* min_data = nullptr;
        void* max_data = nullptr;
        _wrapper->get_minmax_filter_desc(&min_data, &max_data);
        DCHECK(min_data != nullptr);
        DCHECK(max_data != nullptr);
        filter->mutable_min_val()->set_longval(*reinterpret_cast<const int64_t*>(min_data));
        filter->mutable_max_val()->set_longval(*reinterpret_cast<const int64_t*>(max_data));
        return;
    }
    case TYPE_LARGEINT: {
        break;
    }
    case TYPE_FLOAT: {
        break;
    }
    case TYPE_DOUBLE: {
        break;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        break;
    }
    case TYPE_DECIMAL: {
        break;
    }
    case TYPE_DECIMALV2: {
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        break;
    }
    default: {
        break;
    }
    }
    DCHECK(false);
}

template <class T>
Status ShuffleRuntimeFilter::_serialize(T* request, void** data, int* len) {
    request->set_filter_type(get_type(_type));
    if (_type == RuntimeFilterType::BLOOM_FILTER) {
        RETURN_IF_ERROR(_wrapper->get_bloom_filter_desc((char**)data, len));
        DCHECK(data != nullptr);
        request->mutable_bloom_filter()->set_filter_length(*len);
    } else if (_type == RuntimeFilterType::MINMAX_FILTER) {
        auto minmax_filter = request->mutable_minmax_filter();
        to_protobuf(minmax_filter);
    } else {
        return Status::InvalidArgument("not implemented !");
    }
    return Status::OK();
}

Status ShuffleRuntimeFilter::serialize(PMergeFilterRequest* request, void** data, int* len) {
    return _serialize(request, data, len);
}

Status ShuffleRuntimeFilter::serialize(PPublishFilterRequest* request, void** data, int* len) {
    return _serialize(request, data, len);
}

Status ShuffleRuntimeFilter::get_data(void** data, int* len) {
    if (_type == RuntimeFilterType::BLOOM_FILTER) {
        RETURN_IF_ERROR(_wrapper->get_bloom_filter_desc((char**)data, len));
        DCHECK(data != nullptr);
        DCHECK(len > 0);
    }
    return Status::InternalError("unknown type ===");
}

ShuffleRuntimeFilter::~ShuffleRuntimeFilter() {}

Status ShuffleRuntimeFilter::consumer_prepare(const RowDescriptor& desc) {
    return Status::OK();
}

Status ShuffleRuntimeFilter::consumer_close() {
    Expr::close(_target_ctxs, _state);
    return Status::OK();
}

} // namespace doris
