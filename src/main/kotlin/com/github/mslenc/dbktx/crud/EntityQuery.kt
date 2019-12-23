package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.aggr.*
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.buildSelectQuery
import com.github.mslenc.dbktx.util.EntityState.*
import com.github.mslenc.dbktx.expr.CompositeExpr
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.filters.FilterAnd
import com.github.mslenc.dbktx.filters.FilterOr
import com.github.mslenc.dbktx.filters.MatchAnything
import com.github.mslenc.dbktx.filters.MatchNothing
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.util.DelayedLoadState
import com.github.mslenc.dbktx.util.OrderSpec
import java.util.ArrayList
import kotlin.coroutines.suspendCoroutine

interface Query {

}

abstract class QueryImpl {
    private val alias2table = LinkedHashMap<String, TableInQuery<*>>()

    fun isTableAliasTaken(tableAlias: String): Boolean {
        return alias2table.containsKey(tableAlias)
    }

    fun registerTableInQuery(tableInQuery: TableInQuery<*>) {
        alias2table[tableInQuery.tableAlias] = tableInQuery
    }
}

internal class SimpleSelectQueryImpl : QueryImpl()
internal class UpdateQueryImpl : QueryImpl()
internal class InsertQueryImpl : QueryImpl()

enum class FilteringState {
    MATCH_ALL,
    MATCH_NONE,
    MATCH_SOME
}

interface FilterableQuery<E : DbEntity<E, *>>: Query {
    val baseTable : TableInQuery<E>
    fun require(filter: FilterExpr)
    fun filteringState(): FilteringState
}

fun <E: DbEntity<E, *>>
FilterableQuery<E>.exclude(filter: FilterExpr) {
    require(!filter)
}

inline fun <E: DbEntity<E, *>>
FilterableQuery<E>.filter(block: FilterBuilder<E>.() -> FilterExpr) {
    require(createFilter(block))
}

inline fun <E: DbEntity<E, *>, REF: DbEntity<REF, *>>
FilterableQuery<E>.filter(ref: RelToSingle<E, REF>, block: FilterBuilder<REF>.() -> FilterExpr) {
    require(createFilter(ref, block))
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
FilterableQuery<E>.filter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, block: FilterBuilder<REF2>.() -> FilterExpr) {
    require(createFilter(ref1, ref2, block))
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>>
FilterableQuery<E>.filter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, block: FilterBuilder<REF3>.() -> FilterExpr) {
    require(createFilter(ref1, ref2, ref3, block))
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>, REF4: DbEntity<REF4, *>>
FilterableQuery<E>.filter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, ref4: RelToSingle<REF3, REF4>, block: FilterBuilder<REF4>.() -> FilterExpr) {
    require(createFilter(ref1, ref2, ref3, ref4, block))
}


inline fun <E: DbEntity<E, *>>
FilterableQuery<E>.exclude(block: FilterBuilder<E>.() -> FilterExpr) {
    exclude(createFilter(block))
}

inline fun <E: DbEntity<E, *>, REF: DbEntity<REF, *>>
FilterableQuery<E>.exclude(ref: RelToSingle<E, REF>, block: FilterBuilder<REF>.() -> FilterExpr) {
    exclude(createFilter(ref, block))
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
FilterableQuery<E>.exclude(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, block: FilterBuilder<REF2>.() -> FilterExpr) {
    exclude(createFilter(ref1, ref2, block))
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>>
FilterableQuery<E>.exclude(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, block: FilterBuilder<REF3>.() -> FilterExpr) {
    exclude(createFilter(ref1, ref2, ref3, block))
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>, REF4: DbEntity<REF4, *>>
FilterableQuery<E>.exclude(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, ref4: RelToSingle<REF3, REF4>, block: FilterBuilder<REF4>.() -> FilterExpr) {
    exclude(createFilter(ref1, ref2, ref3, ref4, block))
}


inline fun <E: DbEntity<E, *>>
FilterableQuery<E>.createFilter(block: FilterBuilder<E>.() -> FilterExpr): FilterExpr {
    return TableInQueryBoundFilterBuilder(baseTable).block()
}

inline fun <E: DbEntity<E, *>, REF: DbEntity<REF, *>>
FilterableQuery<E>.createFilter(ref: RelToSingle<E, REF>, block: FilterBuilder<REF>.() -> FilterExpr): FilterExpr {
    return TableInQueryBoundFilterBuilder(baseTable.innerJoin(ref)).block()
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
FilterableQuery<E>.createFilter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, block: FilterBuilder<REF2>.() -> FilterExpr): FilterExpr {
    return TableInQueryBoundFilterBuilder(baseTable.innerJoin(ref1).innerJoin(ref2)).block()
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>>
FilterableQuery<E>.createFilter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, block: FilterBuilder<REF3>.() -> FilterExpr): FilterExpr {
    return TableInQueryBoundFilterBuilder(baseTable.innerJoin(ref1).innerJoin(ref2).innerJoin(ref3)).block()
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>, REF4: DbEntity<REF4, *>>
FilterableQuery<E>.createFilter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, ref4: RelToSingle<REF3, REF4>, block: FilterBuilder<REF4>.() -> FilterExpr): FilterExpr {
    return TableInQueryBoundFilterBuilder(baseTable.innerJoin(ref1).innerJoin(ref2).innerJoin(ref3).innerJoin(ref4)).block()
}

fun <E: DbEntity<E, *>>
FilterableQuery<E>.requireAnyOf(filters: Collection<FilterExpr>) {
    require(FilterOr.create(filters))
}

fun <E: DbEntity<E, *>>
FilterableQuery<E>.requireAllOf(filters: Collection<FilterExpr>) {
    require(FilterAnd.create(filters))
}

fun <E: DbEntity<E, *>>
FilterableQuery<E>.excludeWhenAnyOf(exprs: Collection<FilterExpr>) {
    require(!FilterOr.create(exprs))
}

fun <E: DbEntity<E, *>>
FilterableQuery<E>.excludeWhenAllOf(exprs: Collection<FilterExpr>) {
    require(!FilterAnd.create(exprs))
}

internal abstract class FilterableQueryImpl<E: DbEntity<E, *>>(
        internal val table: DbTable<E, *>,
        val db: DbConn) : QueryImpl(), FilterableQuery<E> {

    override val baseTable = makeBaseTable(table)

    protected abstract fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E>
    protected abstract fun checkModifiable()
    internal var filters: FilterExpr = MatchAnything

    override fun require(filter: FilterExpr) {
        checkModifiable()

        this.filters = this.filters and filter
    }

    override fun filteringState(): FilteringState {
        return when (filters) {
            MatchAnything -> FilteringState.MATCH_ALL
            MatchNothing -> FilteringState.MATCH_NONE
            else -> FilteringState.MATCH_SOME
        }
    }
}

interface OrderableQuery<E: DbEntity<E, *>> {
    fun orderBy(order: Expr<*>, ascending: Boolean = true)
    fun <R: DbEntity<R, *>>
        orderBy(ref: RelToSingle<E, R>, order: Expr<*>, ascending: Boolean = true)
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: Expr<*>, ascending: Boolean = true)
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>, R3: DbEntity<R3, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, order: Expr<*>, ascending: Boolean = true)
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>, R3: DbEntity<R3, *>, R4: DbEntity<R4, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, order: Expr<*>, ascending: Boolean = true)
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>, R3: DbEntity<R3, *>, R4: DbEntity<R4, *>, R5: DbEntity<R5, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, ref5: RelToSingle<R4, R5>, order: Expr<*>, ascending: Boolean = true)

    fun orderBy(order: RowProp<E, *>, ascending: Boolean = true)
    fun <R: DbEntity<R, *>>
        orderBy(ref: RelToSingle<E, R>, order: RowProp<R, *>, ascending: Boolean = true)
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: RowProp<R2, *>, ascending: Boolean = true)
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>, R3: DbEntity<R3, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, order: RowProp<R3, *>, ascending: Boolean = true)
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>, R3: DbEntity<R3, *>, R4: DbEntity<R4, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, order: RowProp<R4, *>, ascending: Boolean = true)
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>, R3: DbEntity<R3, *>, R4: DbEntity<R4, *>, R5: DbEntity<R5, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, ref5: RelToSingle<R4, R5>, order: RowProp<R5, *>, ascending: Boolean = true)

    fun offset(offset: Long)
    fun maxRowCount(maxRowCount: Int)
}

interface EntityQuery<E : DbEntity<E, *>>: FilterableQuery<E>, OrderableQuery<E> {
    val db: DbConn

    suspend fun execute(selectForUpdate: Boolean = false): List<E>
    suspend fun countAll(): Long

    fun copy(includeOffsetAndLimit: Boolean = false): EntityQuery<E>
    fun copyAndRemapFilters(dstTable: TableInQuery<E>): FilterExpr

    suspend fun aggregateStream(queryBuilder: AggrStreamBuilder<E>.()->Unit): Long {
        return makeAggregateStreamQuery(queryBuilder).execute()
    }

    fun makeAggregateStreamQuery(queryBuilder: AggrStreamBuilder<E>.()->Unit): AggrStreamQuery<E>
}

class TableInQueryBoundFilterBuilder<E: DbEntity<E, *>>(val table: TableInQuery<E>) : FilterBuilder<E> {
    override fun currentTable(): TableInQuery<E> {
        return table
    }

    override fun <T: Any> bind(prop: RowProp<E, T>): Expr<T> {
        return prop.bindForSelect(table)
    }
}

internal abstract class OrderableFilterableQueryImpl<E : DbEntity<E, *>>(
        table: DbTable<E, *>,
        db: DbConn)
    : FilterableQueryImpl<E>(table, db), OrderableQuery<E> {

    override fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E> {
        return BaseTableInQuery(this, table)
    }

    protected var  _orderBy: ArrayList<OrderSpec<*>>? = null
    internal val orderBy: List<OrderSpec<*>>
        get() {
            return _orderBy ?: emptyList()
        }

    internal var offset: Long? = null
    internal var maxRowCount: Int? = null


    protected fun <R: DbEntity<R, *>>
    addOrder(table: TableInQuery<R>, order: Expr<*>, ascending: Boolean) {
        checkModifiable()

        if (order.isComposite) {
            val comp = order as CompositeExpr<*, *>
            for (i in 1..comp.numParts) {
                addOrder(table, comp.getPart(i), ascending)
            }
        } else {
            if (_orderBy == null)
                _orderBy = ArrayList()

            _orderBy?.add(OrderSpec(table, order, ascending))
        }
    }

    protected fun <R: DbEntity<R, *>>
    addOrder(table: TableInQuery<R>, order: RowProp<R, *>, ascending: Boolean) {
        addOrder(table, order.bindForSelect(table), ascending)
    }

    override fun
    orderBy(order: Expr<*>, ascending: Boolean) {
        addOrder(baseTable, order, ascending)
    }

    override fun <R : DbEntity<R, *>>
    orderBy(ref: RelToSingle<E, R>, order: Expr<*>, ascending: Boolean) {
        addOrder(baseTable.leftJoin(ref), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: Expr<*>, ascending: Boolean) {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, order: Expr<*>, ascending: Boolean) {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, order: Expr<*>, ascending: Boolean) {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>, R5 : DbEntity<R5, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, ref5: RelToSingle<R4, R5>, order: Expr<*>, ascending: Boolean) {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4).leftJoin(ref5), order, ascending)
    }

    override fun
    orderBy(order: RowProp<E, *>, ascending: Boolean) {
        addOrder(baseTable, order, ascending)
    }

    override fun <R : DbEntity<R, *>>
    orderBy(ref: RelToSingle<E, R>, order: RowProp<R, *>, ascending: Boolean) {
        addOrder(baseTable.leftJoin(ref), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: RowProp<R2, *>, ascending: Boolean) {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, order: RowProp<R3, *>, ascending: Boolean) {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, order: RowProp<R4, *>, ascending: Boolean) {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>, R5 : DbEntity<R5, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, ref5: RelToSingle<R4, R5>, order: RowProp<R5, *>, ascending: Boolean) {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4).leftJoin(ref5), order, ascending)
    }

    override fun offset(offset: Long) {
        checkModifiable()

        if (offset < 0)
            throw IllegalArgumentException("offset < 0")

        this.offset = offset
    }

    override fun maxRowCount(maxRowCount: Int) {
        checkModifiable()

        if (maxRowCount < 0)
            throw IllegalArgumentException("maxRowCount < 0")

        this.maxRowCount = maxRowCount
    }
}

internal class EntityQueryImpl<E : DbEntity<E, *>>(
        table: DbTable<E, *>,
        db: DbConn)
    : OrderableFilterableQueryImpl<E>(table, db), EntityQuery<E> {

    override fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E> {
        return BaseTableInQuery(this, table)
    }

    private val queryState = DelayedLoadState<List<E>>(db.scope)
    private val countState = DelayedLoadState<Long>(db.scope)

    override suspend fun execute(selectForUpdate: Boolean): List<E> {
        return when (queryState.state) {
            LOADED  -> queryState.value
            LOADING -> suspendCoroutine(queryState::addReceiver)
            INITIAL -> queryState.startLoading { db.executeSelect(this, selectForUpdate) }
        }
    }

    override suspend fun countAll(): Long {
        return when (countState.state) {
            LOADED  -> countState.value
            LOADING -> suspendCoroutine(countState::addReceiver)
            INITIAL -> countState.startLoading { db.executeCount(this) }
        }
    }

    override fun checkModifiable() {
        if (queryState.state !== INITIAL || countState.state !== INITIAL)
            throw IllegalStateException("Already querying")
    }

    override fun copy(includeOffsetAndLimit: Boolean): EntityQuery<E> {
        val newQuery = EntityQueryImpl(table, db)
        val remapper = TableRemapper(newQuery.baseTable.query)
        remapper.addExplicitMapping(baseTable, newQuery.baseTable)

        newQuery.filters = filters.remap(remapper)
        _orderBy?.let { orderBy ->
            val newOrder = ArrayList<OrderSpec<*>>()
            orderBy.forEach { newOrder.add(it.remap(remapper)) }
            newQuery._orderBy = newOrder
        }

        if (includeOffsetAndLimit) {
            newQuery.offset = offset
            newQuery.maxRowCount = maxRowCount
        }

        return newQuery
    }

    override fun copyAndRemapFilters(dstTable: TableInQuery<E>): FilterExpr {
        val remapper = TableRemapper(dstTable.query)
        remapper.addExplicitMapping(baseTable, dstTable)
        return filters.remap(remapper)
    }

    override fun makeAggregateStreamQuery(queryBuilder: AggrStreamBuilder<E>.() -> Unit): AggrStreamQuery<E> {
        val query = table.makeAggregateStreamQuery(db, {}) as AggrStreamImpl

        val remapper = TableRemapper(query)
        remapper.addExplicitMapping(baseTable, query.baseTable)
        query.filter { filters.remap(remapper) }

        query.expand(queryBuilder)

        return query
    }

    override fun toString(): String {
        return buildSelectQuery(this, false).getSql()
    }
}
