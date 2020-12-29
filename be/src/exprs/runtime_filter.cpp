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
#include "runtime/type_limit.h"

namespace doris {
// only used in Runtime Filter
class MinMaxFuncBase {
public:
    virtual void insert(void* data) = 0;
    virtual bool find(void* data) = 0;
    virtual bool is_empty() = 0;
    virtual const void* get_max() = 0;
    virtual const void* get_min() = 0;
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
        // static_cast<MinMaxNumFunc<T>*>(minmax_func)->_min;
        if (other_minmax->_min < _min) {
            _min = other_minmax->_min;
        }
        if (other_minmax->_max < _max) {
            _max = other_minmax->_max;
        }
        _empty = true;
        return Status::OK();
    }

    virtual bool is_empty() { return _empty; }

    virtual const void* get_max() { return &_max; }

    virtual const void* get_min() { return &_min; }

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
class RuntimePredicateWrapper {
public:
    RuntimePredicateWrapper(RuntimeState* state, MemTracker* tracker, ObjectPool* pool,
                            const RuntimeFilterParams* params)
            : _state(state),
              _tracker(tracker),
              _pool(pool),
              _filter_type(params->filter_type),
              _expr_ctx(params->prob_expr_ctx) {
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

            if (params->bloom_filter_size > 0) {
                if (!_bloomfilter_func->init_with_fixed_length(params->bloom_filter_size).ok()) {
                    _bloomfilter_func.reset();
                }
            } else {
                if (!_bloomfilter_func->init(params->hash_table_size).ok()) {
                    _bloomfilter_func.reset();
                }
            }
            break;
        }
        default:
            break;
        }
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
            if (_bloomfilter_func != nullptr) {
                _bloomfilter_func->insert(data);
            }
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

    Status assign(const PBloomFilter* bloom_filter, const char* data) {
        _bloomfilter_func.reset(new BloomFilterFuncBase(_tracker));
        return _bloomfilter_func->assign(data, bloom_filter->filter_length() + 1);
    }

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
    // RuntimeFilterType filter_type,
    // size_t prob_index,
    // ExprContext* prob_expr_ctx,
    // int64_t hash_table_size

    switch (params->filter_type) {
    case RuntimeFilterType::IN_FILTER:
    case RuntimeFilterType::MINMAX_FILTER:
    case RuntimeFilterType::BLOOM_FILTER: {
        _runtime_preds[params->prob_expr_index].push_back(
                _pool->add(new RuntimePredicateWrapper(_state, _mem_tracker, _pool, params)));
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
        : _is_ready(false), _state(state), _mem_tracker(tracker), _pool(pool) {}

void ShuffleRuntimeFilter::insert(TupleRow* row) {}

bool ShuffleRuntimeFilter::await() {
    int64_t wait_times_ms = 1000;
    if (!_is_ready) {
        std::unique_lock<std::mutex> lock(_inner_mutex);
        return _inner_cv.wait_for(lock, std::chrono::milliseconds(wait_times_ms),
                                  [this] { return this->_is_ready; });
    }
    return true;
}

void ShuffleRuntimeFilter::signal() {
    _inner_cv.notify_one();
}

Status ShuffleRuntimeFilter::merge_from(const ShuffleRuntimeFilter& shuffle_runtime_filter) {
    if (shuffle_runtime_filter.type() != type()) {
        return Status::InvalidArgument("filter type error");
    }
    return _wrapper->merge(shuffle_runtime_filter._wrapper);
}

Status ShuffleRuntimeFilter::update_filter(const UpdateRuntimeFilterParams* param) {
    int filter_mem_limit = 1024 * 1024 * 2;
    ObjectPool pool;
    MemTracker tracker(filter_mem_limit);
    ShuffleRuntimeFilter filter(nullptr, &tracker, &pool);
    RETURN_IF_ERROR(filter.init_with_desc(_runtime_filter_desc));
    RETURN_IF_ERROR(filter.apply_init_update_params(param));
    return _wrapper->merge(filter._wrapper);
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
    //_runtime_filter_desc->planid_to_target_expr;
    RuntimeFilterParams params;
    params.filter_type = _type;
    params.prob_expr_ctx = target_ctx;
    params.bloom_filter_size = desc->bloom_filter_size_bytes;
    _wrapper = _pool->add(new RuntimePredicateWrapper(_state, _mem_tracker, _pool, &params));
    return Status::OK();
}

Status ShuffleRuntimeFilter::apply_init_update_params(const UpdateRuntimeFilterParams* param) {
    DCHECK(_wrapper != nullptr);
    int filter_type = param->publish_request->filter_type();
    switch (filter_type) {
    case FilterType::BLOOM_FILTER: {
        DCHECK(param->publish_request->has_bloom_filter());
        return _wrapper->assign(&param->publish_request->bloom_filter(), param->data);
    }
    case FilterType::MINMAX_FILTER:
        return Status::InvalidArgument("Unsupport min max filter");
    default:
        return Status::InvalidArgument("unknow filter type");
    }
    return Status::OK();
}

} // namespace doris
