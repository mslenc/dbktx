package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.asyncdb.DbValue
import com.github.mslenc.dbktx.schema.Column
import java.lang.IllegalArgumentException

class AggregateRow(private val rowData: DbRow, private val bindings: Map<Any, BoundAggregateExpr<*>>) {
    operator fun get(indexInRow: Int): DbValue {
        return rowData.getValue(indexInRow)
    }

    operator fun <T : Any> get(column: Column<*, T>): T? {
        val binding = bindings[column] ?: throw IllegalArgumentException("Column $column is not in this query result")
        @Suppress("UNCHECKED_CAST")
        val typedBinding = binding as BoundAggregateExpr<T>
        return typedBinding.invoke(this)
    }

    operator fun <T : Any> get(expr: AggregateExpr<*, T>): T? {
        val binding = bindings[expr] ?: throw IllegalArgumentException("Expression $expr is not in this query result")
        @Suppress("UNCHECKED_CAST")
        val typedBinding = binding as BoundAggregateExpr<T>
        return typedBinding.invoke(this)
    }

    operator fun <T : Any> get(boundExpr: BoundAggregateExpr<T>): T? {
        return boundExpr(this)
    }

    fun getValue(expr: AggregateExpr<*, *>): DbValue {
        val binding = bindings[expr] ?: throw IllegalArgumentException("Expression $expr is not in this query result")
        return rowData.getValue(binding.indexInRow)
    }
}