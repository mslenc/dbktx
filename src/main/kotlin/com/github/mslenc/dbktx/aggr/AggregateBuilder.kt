package com.github.mslenc.dbktx.aggr

import com.github.mslenc.dbktx.crud.FilterBuilder
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.crud.TableInQueryBoundFilterBuilder
import com.github.mslenc.dbktx.expr.ExprBoolean
import com.github.mslenc.dbktx.schema.*

interface AggregateBuilder<E : DbEntity<E, *>> {
    fun <T : Any> groupBy(column: Column<E, T>): BoundAggregateExpr<T>
    fun <T : Any> select(expr: AggregateExpr<E, T>): BoundAggregateExpr<T>

    operator fun <T : Any> AggregateExpr<E, T>.unaryPlus(): BoundAggregateExpr<T> {
        return select(this)
    }

    operator fun <T : Any> Column<E, T>.unaryPlus(): BoundAggregateExpr<T> {
        return groupBy(this)
    }

    fun filter(block: FilterBuilder<E>.() -> ExprBoolean)
    fun exclude(block: FilterBuilder<E>.() -> ExprBoolean)

    fun <REF : DbEntity<REF, *>> innerJoin(ref: RelToOne<E, REF>, block: AggregateBuilder<REF>.() -> Unit)
    fun <REF : DbEntity<REF, *>> innerJoin(set: RelToMany<E, REF>, block: AggregateBuilder<REF>.() -> Unit)

    fun <REF : DbEntity<REF, *>> leftJoin(ref: RelToOne<E, REF>, block: AggregateBuilder<REF>.() -> Unit)
    fun <REF : DbEntity<REF, *>> leftJoin(set: RelToMany<E, REF>, block: AggregateBuilder<REF>.() -> Unit)
}

internal class AggregateBuilderImpl<E: DbEntity<E, *>>(val query: AggregateQueryImpl<*>, val tableInQuery: TableInQuery<E>) : AggregateBuilder<E> {
    override fun <REF : DbEntity<REF, *>>
    innerJoin(ref: RelToOne<E, REF>, block: AggregateBuilder<REF>.() -> Unit) {
        val subTable = tableInQuery.innerJoin(ref)
        val subBuilder = AggregateBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>>
    leftJoin(ref: RelToOne<E, REF>, block: AggregateBuilder<REF>.() -> Unit) {
        val subTable = tableInQuery.leftJoin(ref)
        val subBuilder = AggregateBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>>
    innerJoin(set: RelToMany<E, REF>, block: AggregateBuilder<REF>.() -> Unit) {
        val subTable = tableInQuery.innerJoin(set)
        val subBuilder = AggregateBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>>
    leftJoin(set: RelToMany<E, REF>, block: AggregateBuilder<REF>.() -> Unit) {
        val subTable = tableInQuery.leftJoin(set)
        val subBuilder = AggregateBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun filter(block: FilterBuilder<E>.() -> ExprBoolean) {
        val filterBuilder = TableInQueryBoundFilterBuilder(tableInQuery)
        val filterExpr = filterBuilder.block()
        query.addFilter(filterExpr)
    }

    override fun exclude(block: FilterBuilder<E>.() -> ExprBoolean) {
        val filterBuilder = TableInQueryBoundFilterBuilder(tableInQuery)
        val filterExpr = filterBuilder.block()
        query.addFilter(filterExpr.not())
    }

    override fun <T : Any> groupBy(column: Column<E, T>): BoundAggregateExpr<T> {
        return query.addSelectAndGroupBy(column.bindForSelect(tableInQuery), column.sqlType)
    }

    override fun <T : Any> select(expr: AggregateExpr<E, T>): BoundAggregateExpr<T> {
        return query.addSelect(expr, tableInQuery)
    }
}