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

    fun exclude(filter: FilterExpr) {
        require(!filter)
    }

    fun filter(block: FilterBuilder<E>.() -> FilterExpr) {
        require(createFilter(block))
    }

    fun <REF: DbEntity<REF, *>>
    filter(ref: RelToSingle<E, REF>, block: FilterBuilder<REF>.() -> FilterExpr) {
        require(createFilter(ref, block))
    }

    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
    filter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, block: FilterBuilder<REF2>.() -> FilterExpr) {
        require(createFilter(ref1, ref2, block))
    }

    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>>
    filter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, block: FilterBuilder<REF3>.() -> FilterExpr) {
        require(createFilter(ref1, ref2, ref3, block))
    }

    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>, REF4: DbEntity<REF4, *>>
    filter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, ref4: RelToSingle<REF3, REF4>, block: FilterBuilder<REF4>.() -> FilterExpr) {
        require(createFilter(ref1, ref2, ref3, ref4, block))
    }


    fun exclude(block: FilterBuilder<E>.() -> FilterExpr) {
        exclude(createFilter(block))
    }

    fun <REF: DbEntity<REF, *>>
    exclude(ref: RelToSingle<E, REF>, block: FilterBuilder<REF>.() -> FilterExpr) {
        exclude(createFilter(ref, block))
    }

    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
    exclude(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, block: FilterBuilder<REF2>.() -> FilterExpr) {
        exclude(createFilter(ref1, ref2, block))
    }

    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>>
    exclude(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, block: FilterBuilder<REF3>.() -> FilterExpr) {
        exclude(createFilter(ref1, ref2, ref3, block))
    }

    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>, REF4: DbEntity<REF4, *>>
    exclude(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, ref4: RelToSingle<REF3, REF4>, block: FilterBuilder<REF4>.() -> FilterExpr) {
        exclude(createFilter(ref1, ref2, ref3, ref4, block))
    }


    fun createFilter(block: FilterBuilder<E>.() -> FilterExpr): FilterExpr {
        return TableInQueryBoundFilterBuilder(baseTable).block()
    }

    fun <REF: DbEntity<REF, *>>
    createFilter(ref: RelToSingle<E, REF>, block: FilterBuilder<REF>.() -> FilterExpr): FilterExpr {
        return TableInQueryBoundFilterBuilder(baseTable.innerJoin(ref)).block()
    }

    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
    createFilter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, block: FilterBuilder<REF2>.() -> FilterExpr): FilterExpr {
        return TableInQueryBoundFilterBuilder(baseTable.innerJoin(ref1).innerJoin(ref2)).block()
    }

    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>>
    createFilter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, block: FilterBuilder<REF3>.() -> FilterExpr): FilterExpr {
        return TableInQueryBoundFilterBuilder(baseTable.innerJoin(ref1).innerJoin(ref2).innerJoin(ref3)).block()
    }

    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>, REF3: DbEntity<REF3, *>, REF4: DbEntity<REF4, *>>
    createFilter(ref1: RelToSingle<E, REF1>, ref2: RelToSingle<REF1, REF2>, ref3: RelToSingle<REF2, REF3>, ref4: RelToSingle<REF3, REF4>, block: FilterBuilder<REF4>.() -> FilterExpr): FilterExpr {
        return TableInQueryBoundFilterBuilder(baseTable.innerJoin(ref1).innerJoin(ref2).innerJoin(ref3).innerJoin(ref4)).block()
    }

    fun requireAnyOf(filters: Collection<FilterExpr>) {
        require(FilterOr.create(filters))
    }

    fun requireAllOf(filters: Collection<FilterExpr>) {
        require(FilterAnd.create(filters))
    }

    fun excludeWhenAnyOf(exprs: Collection<FilterExpr>) {
        require(!FilterOr.create(exprs))
    }

    fun excludeWhenAllOf(exprs: Collection<FilterExpr>) {
        require(!FilterAnd.create(exprs))
    }
}

internal abstract class FilterableQueryImpl<E: DbEntity<E, *>>(
        internal val table: DbTable<E, *>,
        internal val loader: DbConn) : QueryImpl(), FilterableQuery<E> {

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
    fun orderBy(order: Expr<in E, *>, ascending: Boolean = true): OrderableQuery<E>
    fun <R: DbEntity<R, *>>
        orderBy(ref: RelToSingle<E, R>, order: Expr<in R, *>, ascending: Boolean = true): OrderableQuery<E>
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: Expr<R2, *>, ascending: Boolean = true): OrderableQuery<E>
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>, R3: DbEntity<R3, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, order: Expr<R3, *>, ascending: Boolean = true): OrderableQuery<E>
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>, R3: DbEntity<R3, *>, R4: DbEntity<R4, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, order: Expr<R4, *>, ascending: Boolean = true): OrderableQuery<E>
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>, R3: DbEntity<R3, *>, R4: DbEntity<R4, *>, R5: DbEntity<R5, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, ref5: RelToSingle<R4, R5>, order: Expr<R5, *>, ascending: Boolean = true): OrderableQuery<E>

    fun orderBy(order: RowProp<E, *>, ascending: Boolean = true): OrderableQuery<E>
    fun <R: DbEntity<R, *>>
        orderBy(ref: RelToSingle<E, R>, order: RowProp<R, *>, ascending: Boolean = true): OrderableQuery<E>
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: RowProp<R2, *>, ascending: Boolean = true): OrderableQuery<E>
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>, R3: DbEntity<R3, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, order: RowProp<R3, *>, ascending: Boolean = true): OrderableQuery<E>
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>, R3: DbEntity<R3, *>, R4: DbEntity<R4, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, order: RowProp<R4, *>, ascending: Boolean = true): OrderableQuery<E>
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>, R3: DbEntity<R3, *>, R4: DbEntity<R4, *>, R5: DbEntity<R5, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, ref5: RelToSingle<R4, R5>, order: RowProp<R5, *>, ascending: Boolean = true): OrderableQuery<E>

    fun offset(offset: Long): OrderableQuery<E>
    fun maxRowCount(maxRowCount: Int): OrderableQuery<E>
}

interface EntityQuery<E : DbEntity<E, *>>: FilterableQuery<E>, OrderableQuery<E> {
    val db: DbConn

    suspend fun run(selectForUpdate: Boolean = false): List<E>
    suspend fun countAll(): Long

    fun copy(includeOffsetAndLimit: Boolean = false): EntityQuery<E>
    fun copyAndRemapFilters(dstTable: TableInQuery<E>): FilterExpr

    suspend fun <OUT: Any> aggregateInto(factory: ()->OUT, queryBuilder: AggrListBuilder<OUT, E>.()->Unit): List<OUT> {
        return makeAggregateListQuery(factory, queryBuilder).run()
    }

    suspend fun aggregateStream(queryBuilder: AggrStreamBuilder<E>.()->Unit): Long {
        return makeAggregateStreamQuery(queryBuilder).run()
    }

    fun <OUT: Any> makeAggregateListQuery(factory: ()->OUT, queryBuilder: AggrListBuilder<OUT, E>.()->Unit): AggrListQuery<OUT, E>
    fun makeAggregateStreamQuery(queryBuilder: AggrStreamBuilder<E>.()->Unit): AggrStreamQuery<E>
}

class TableInQueryBoundFilterBuilder<E: DbEntity<E, *>>(val table: TableInQuery<E>) : FilterBuilder<E> {
    override fun currentTable(): TableInQuery<E> {
        return table
    }

    override fun <T: Any> bind(prop: RowProp<E, T>): Expr<E, T> {
        return prop.bindForSelect(table)
    }
}

internal abstract class OrderableFilterableQueryImpl<E : DbEntity<E, *>>(
        table: DbTable<E, *>,
        loader: DbConn)
    : FilterableQueryImpl<E>(table, loader), OrderableQuery<E> {

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
    addOrder(table: TableInQuery<R>, order: Expr<in R, *>, ascending: Boolean) {
        checkModifiable()

        if (order.isComposite) {
            val comp = order as CompositeExpr
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

    protected fun setOffset(offset: Long) {
        checkModifiable()

        if (offset < 0)
            throw IllegalArgumentException("offset < 0")

        this.offset = offset
    }

    protected fun setMaxRowCount(maxRowCount: Int) {
        checkModifiable()

        if (maxRowCount < 0)
            throw IllegalArgumentException("maxRowCount < 0")

        this.maxRowCount = maxRowCount
    }
}

internal class EntityQueryImpl<E : DbEntity<E, *>>(
        table: DbTable<E, *>,
        loader: DbConn)
    : OrderableFilterableQueryImpl<E>(table, loader), EntityQuery<E> {

    override fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E> {
        return BaseTableInQuery(this, table)
    }

    override val db: DbConn
        get() = loader

    private val queryState = DelayedLoadState<List<E>>(loader.scope)
    private val countState = DelayedLoadState<Long>(loader.scope)

    override suspend fun run(selectForUpdate: Boolean): List<E> {
        return when (queryState.state) {
            LOADED  -> queryState.value
            LOADING -> suspendCoroutine(queryState::addReceiver)
            INITIAL -> queryState.startLoading { loader.executeSelect(this, selectForUpdate) }
        }
    }

    override suspend fun countAll(): Long {
        return when (countState.state) {
            LOADED  -> countState.value
            LOADING -> suspendCoroutine(countState::addReceiver)
            INITIAL -> countState.startLoading { loader.executeCount(this) }
        }
    }

    override fun checkModifiable() {
        if (queryState.state !== INITIAL || countState.state !== INITIAL)
            throw IllegalStateException("Already querying")
    }

    override fun
    orderBy(order: Expr<in E, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable, order, ascending)
        return this
    }

    override fun <R : DbEntity<R, *>>
    orderBy(ref: RelToSingle<E, R>, order: Expr<in R, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable.leftJoin(ref), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: Expr<R2, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, order: Expr<R3, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, order: Expr<R4, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>, R5 : DbEntity<R5, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, ref5: RelToSingle<R4, R5>, order: Expr<R5, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4).leftJoin(ref5), order, ascending)
        return this
    }

    override fun
    orderBy(order: RowProp<E, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable, order, ascending)
        return this
    }

    override fun <R : DbEntity<R, *>>
    orderBy(ref: RelToSingle<E, R>, order: RowProp<R, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable.leftJoin(ref), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: RowProp<R2, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, order: RowProp<R3, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, order: RowProp<R4, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>, R5 : DbEntity<R5, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, ref5: RelToSingle<R4, R5>, order: RowProp<R5, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4).leftJoin(ref5), order, ascending)
        return this
    }

    override fun offset(offset: Long): EntityQuery<E> {
        setOffset(offset)
        return this
    }

    override fun maxRowCount(maxRowCount: Int): EntityQuery<E> {
        setMaxRowCount(maxRowCount)
        return this
    }

    override fun copy(includeOffsetAndLimit: Boolean): EntityQuery<E> {
        val newQuery = EntityQueryImpl(table, loader)
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

    override fun <OUT : Any> makeAggregateListQuery(factory: () -> OUT, queryBuilder: AggrListBuilder<OUT, E>.() -> Unit): AggrListQuery<OUT, E> {
        val query = table.makeAggregateListQuery(db, factory, {}) as AggrListImpl

        val remapper = TableRemapper(query)
        remapper.addExplicitMapping(baseTable, query.baseTable)
        query.filter { filters.remap(remapper) }

        query.expand(queryBuilder)

        return query
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
