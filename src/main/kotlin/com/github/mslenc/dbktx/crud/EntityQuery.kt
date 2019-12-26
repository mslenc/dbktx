package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.aggr.*
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.buildSelectQuery
import com.github.mslenc.dbktx.expr.*
import com.github.mslenc.dbktx.util.EntityState.*
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
    val aggregatesAllowed: Boolean
}

abstract class QueryImpl : Query {
    private val alias2table = LinkedHashMap<String, TableInQuery<*>>()

    fun isTableAliasTaken(tableAlias: String): Boolean {
        return alias2table.containsKey(tableAlias)
    }

    fun registerTableInQuery(tableInQuery: TableInQuery<*>) {
        alias2table[tableInQuery.tableAlias] = tableInQuery
    }
}

internal class SimpleSelectQueryImpl : QueryImpl() {
    override val aggregatesAllowed: Boolean
        get() = false
}

internal class UpdateQueryImpl : QueryImpl() {
    override val aggregatesAllowed: Boolean
        get() = false
}

internal class InsertQueryImpl : QueryImpl() {
    override val aggregatesAllowed: Boolean
        get() = false
}

enum class FilteringState {
    MATCH_ALL,
    MATCH_NONE,
    MATCH_SOME
}

interface FilterableQuery<E : DbEntity<E, *>>: Query {
    val table : TableInQuery<E>
    fun require(filter: Expr<Boolean>)
    fun filteringState(): FilteringState
}

fun <E: DbEntity<E, *>>
FilterableQuery<E>.exclude(filter: Expr<Boolean>) {
    require(!filter)
}

inline fun <E: DbEntity<E, *>>
FilterableQuery<E>.filter(block: ExprBuilder<E>.() -> Expr<Boolean>) {
    require(createFilter(block))
}

inline fun <E: DbEntity<E, *>, REF: DbEntity<REF, *>>
FilterableQuery<E>.filter(ref: RelToSingle<E, REF>, block: ExprBuilder<REF>.() -> Expr<Boolean>) {
    require(createFilter(ref, block))
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
FilterableQuery<E>.filter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, block: ExprBuilder<REF2>.() -> Expr<Boolean>) {
    require(createFilter(ref1, ref2, block))
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>>
FilterableQuery<E>.filter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, block: ExprBuilder<REF3>.() -> Expr<Boolean>) {
    require(createFilter(ref1, ref2, ref3, block))
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>, REF4: DbEntity<REF4, *>>
FilterableQuery<E>.filter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, ref4: RelToSingle<REF3, REF4>, block: ExprBuilder<REF4>.() -> Expr<Boolean>) {
    require(createFilter(ref1, ref2, ref3, ref4, block))
}


inline fun <E: DbEntity<E, *>>
FilterableQuery<E>.exclude(block: ExprBuilder<E>.() -> Expr<Boolean>) {
    exclude(createFilter(block))
}

inline fun <E: DbEntity<E, *>, REF: DbEntity<REF, *>>
FilterableQuery<E>.exclude(ref: RelToSingle<E, REF>, block: ExprBuilder<REF>.() -> Expr<Boolean>) {
    exclude(createFilter(ref, block))
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
FilterableQuery<E>.exclude(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, block: ExprBuilder<REF2>.() -> Expr<Boolean>) {
    exclude(createFilter(ref1, ref2, block))
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>>
FilterableQuery<E>.exclude(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, block: ExprBuilder<REF3>.() -> Expr<Boolean>) {
    exclude(createFilter(ref1, ref2, ref3, block))
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>, REF4: DbEntity<REF4, *>>
FilterableQuery<E>.exclude(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, ref4: RelToSingle<REF3, REF4>, block: ExprBuilder<REF4>.() -> Expr<Boolean>) {
    exclude(createFilter(ref1, ref2, ref3, ref4, block))
}


inline fun <E: DbEntity<E, *>>
FilterableQuery<E>.createFilter(block: ExprBuilder<E>.() -> Expr<Boolean>): Expr<Boolean> {
    return table.newExprBuilder().block()
}

inline fun <E: DbEntity<E, *>, REF: DbEntity<REF, *>>
FilterableQuery<E>.createFilter(ref: RelToSingle<E, REF>, block: ExprBuilder<REF>.() -> Expr<Boolean>): Expr<Boolean> {
    return table.innerJoin(ref).newExprBuilder().block()
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
FilterableQuery<E>.createFilter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, block: ExprBuilder<REF2>.() -> Expr<Boolean>): Expr<Boolean> {
    return table.innerJoin(ref1).innerJoin(ref2).newExprBuilder().block()
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>>
FilterableQuery<E>.createFilter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, block: ExprBuilder<REF3>.() -> Expr<Boolean>): Expr<Boolean> {
    return table.innerJoin(ref1).innerJoin(ref2).innerJoin(ref3).newExprBuilder().block()
}

inline fun <E: DbEntity<E, *>, REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>, REF4: DbEntity<REF4, *>>
FilterableQuery<E>.createFilter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, ref4: RelToSingle<REF3, REF4>, block: ExprBuilder<REF4>.() -> Expr<Boolean>): Expr<Boolean> {
    return table.innerJoin(ref1).innerJoin(ref2).innerJoin(ref3).innerJoin(ref4).newExprBuilder().block()
}

fun <E: DbEntity<E, *>>
FilterableQuery<E>.requireAnyOf(filters: Collection<Expr<Boolean>>) {
    require(FilterOr.create(filters))
}

fun <E: DbEntity<E, *>>
FilterableQuery<E>.requireAllOf(filters: Collection<Expr<Boolean>>) {
    require(FilterAnd.create(filters))
}

fun <E: DbEntity<E, *>>
FilterableQuery<E>.excludeWhenAnyOf(exprs: Collection<Expr<Boolean>>) {
    require(!FilterOr.create(exprs))
}

fun <E: DbEntity<E, *>>
FilterableQuery<E>.excludeWhenAllOf(exprs: Collection<Expr<Boolean>>) {
    require(!FilterAnd.create(exprs))
}

internal abstract class FilterableQueryImpl<E: DbEntity<E, *>>(
        table: DbTable<E, *>,
        val db: DbConn) : QueryImpl(), FilterableQuery<E> {

    override val table = makeBaseTable(table)

    protected abstract fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E>
    protected abstract fun checkModifiable()
    internal var filters: Expr<Boolean> = MatchAnything

    override fun require(filter: Expr<Boolean>) {
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
    fun copyAndRemapFilters(dstTable: TableInQuery<E>): Expr<Boolean>

    suspend fun aggregateStream(queryBuilder: AggrStreamBuilder<E>.()->Unit): Long {
        return makeAggregateStreamQuery(queryBuilder).execute()
    }

    fun makeAggregateStreamQuery(queryBuilder: AggrStreamBuilder<E>.()->Unit): AggrStreamQuery<E>
}

internal abstract class OrderableFilterableQueryImpl<E : DbEntity<E, *>>(
        table: DbTable<E, *>,
        db: DbConn)
    : FilterableQueryImpl<E>(table, db), OrderableQuery<E> {

    override fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E> {
        return BaseTableInQuery(this, table)
    }

    protected var  _orderBy: ArrayList<OrderSpec>? = null
    internal val orderBy: List<OrderSpec>
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

            _orderBy?.add(OrderSpec(order, ascending))
        }
    }

    protected fun <R: DbEntity<R, *>>
    addOrder(table: TableInQuery<R>, order: RowProp<R, *>, ascending: Boolean) {
        addOrder(table, order.bindForSelect(table), ascending)
    }

    override fun
    orderBy(order: Expr<*>, ascending: Boolean) {
        addOrder(table, order, ascending)
    }

    override fun <R : DbEntity<R, *>>
    orderBy(ref: RelToSingle<E, R>, order: Expr<*>, ascending: Boolean) {
        addOrder(table.leftJoin(ref), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: Expr<*>, ascending: Boolean) {
        addOrder(table.leftJoin(ref1).leftJoin(ref2), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, order: Expr<*>, ascending: Boolean) {
        addOrder(table.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, order: Expr<*>, ascending: Boolean) {
        addOrder(table.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>, R5 : DbEntity<R5, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, ref5: RelToSingle<R4, R5>, order: Expr<*>, ascending: Boolean) {
        addOrder(table.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4).leftJoin(ref5), order, ascending)
    }

    override fun
    orderBy(order: RowProp<E, *>, ascending: Boolean) {
        addOrder(table, order, ascending)
    }

    override fun <R : DbEntity<R, *>>
    orderBy(ref: RelToSingle<E, R>, order: RowProp<R, *>, ascending: Boolean) {
        addOrder(table.leftJoin(ref), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: RowProp<R2, *>, ascending: Boolean) {
        addOrder(table.leftJoin(ref1).leftJoin(ref2), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, order: RowProp<R3, *>, ascending: Boolean) {
        addOrder(table.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, order: RowProp<R4, *>, ascending: Boolean) {
        addOrder(table.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4), order, ascending)
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>, R5 : DbEntity<R5, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, ref5: RelToSingle<R4, R5>, order: RowProp<R5, *>, ascending: Boolean) {
        addOrder(table.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4).leftJoin(ref5), order, ascending)
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

    override val aggregatesAllowed: Boolean
        get() = false

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
        val newQuery = EntityQueryImpl(table.table, db)
        val remapper = TableRemapper(newQuery.table.query)
        remapper.addExplicitMapping(table, newQuery.table)

        newQuery.filters = filters.remap(remapper)
        _orderBy?.let { orderBy ->
            val newOrder = ArrayList<OrderSpec>()
            orderBy.forEach { newOrder.add(it.remap(remapper)) }
            newQuery._orderBy = newOrder
        }

        if (includeOffsetAndLimit) {
            newQuery.offset = offset
            newQuery.maxRowCount = maxRowCount
        }

        return newQuery
    }

    override fun copyAndRemapFilters(dstTable: TableInQuery<E>): Expr<Boolean> {
        val remapper = TableRemapper(dstTable.query)
        remapper.addExplicitMapping(table, dstTable)
        return filters.remap(remapper)
    }

    override fun makeAggregateStreamQuery(queryBuilder: AggrStreamBuilder<E>.() -> Unit): AggrStreamQuery<E> {
        val query = table.table.makeAggregateStreamQuery(db) {} as AggrStreamImpl

        val remapper = TableRemapper(query)
        remapper.addExplicitMapping(table, query.table)
        query.filter { filters.remap(remapper) }

        query.expand(queryBuilder)

        return query
    }

    override fun toString(): String {
        return buildSelectQuery(this, false).getSql()
    }
}
