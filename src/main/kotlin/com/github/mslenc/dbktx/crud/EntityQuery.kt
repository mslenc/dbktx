package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.aggr.*
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.buildSelectQuery
import com.github.mslenc.dbktx.util.EntityState.*
import com.github.mslenc.dbktx.expr.CompositeExpr
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.filters.FilterBoolean
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
        alias2table.put(tableInQuery.tableAlias, tableInQuery)
    }
}

internal class SimpleSelectQueryImpl : QueryImpl()
internal class UpdateQueryImpl : QueryImpl()
internal class InsertQueryImpl : QueryImpl()

interface FilterableQuery<E : DbEntity<E, *>>: Query {
    val baseTable : TableInQuery<E>

    fun filter(block: FilterBuilder<E>.() -> FilterExpr): FilterableQuery<E>
    fun <REF: DbEntity<REF, *>>
        filter(ref: RelToOne<E, REF>, block: FilterBuilder<REF>.() -> FilterExpr): FilterableQuery<E>
    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
        filter(ref1: RelToOne<E, REF1>, ref2: RelToOne<REF1, REF2>, block: FilterBuilder<REF2>.() -> FilterExpr): FilterableQuery<E>


    fun exclude(block: FilterBuilder<E>.() -> FilterExpr): FilterableQuery<E>
    fun <REF: DbEntity<REF, *>>
        exclude(ref: RelToOne<E, REF>, block: FilterBuilder<REF>.() -> FilterExpr): FilterableQuery<E>
    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
        exclude(ref1: RelToOne<E, REF1>, ref2: RelToOne<REF1, REF2>, block: FilterBuilder<REF2>.() -> FilterExpr): FilterableQuery<E>
}

internal abstract class FilterableQueryImpl<E: DbEntity<E, *>>(
        internal val table: DbTable<E, *>,
        internal val loader: DbConn) : QueryImpl(), FilterableQuery<E> {

    override val baseTable = makeBaseTable(table)

    protected abstract fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E>
    protected abstract fun checkModifiable()
    internal var filters: FilterExpr? = null

    private fun <E: DbEntity<E, *>>
        doFilter(tableInQuery: TableInQuery<E>, negate: Boolean, block: FilterBuilder<E>.() -> FilterExpr) {

        checkModifiable()

        val filterBuilder = TableInQueryBoundFilterBuilder(tableInQuery)
        val filter = filterBuilder.block()
        val finalFilter = if (negate) !filter else filter

        addFilter(finalFilter)
    }

    internal fun addFilter(filter: FilterExpr) {
        checkModifiable()

        val existing = this.filters

        if (existing != null) {
            this.filters = FilterBoolean.create(existing, FilterBoolean.Op.AND, filter)
        } else {
            this.filters = filter
        }
    }

    override fun filter(block: FilterBuilder<E>.() -> FilterExpr): FilterableQuery<E> {
        doFilter(baseTable, false, block)
        return this
    }

    override fun <REF : DbEntity<REF, *>> filter(ref: RelToOne<E, REF>, block: FilterBuilder<REF>.() -> FilterExpr): FilterableQuery<E> {
        doFilter(baseTable.innerJoin(ref), false, block)
        return this
    }

    override fun <REF1 : DbEntity<REF1, *>, REF2 : DbEntity<REF2, *>> filter(ref1: RelToOne<E, REF1>, ref2: RelToOne<REF1, REF2>, block: FilterBuilder<REF2>.() -> FilterExpr): FilterableQuery<E> {
        doFilter(baseTable.innerJoin(ref1).innerJoin(ref2), false, block)
        return this
    }

    override fun exclude(block: FilterBuilder<E>.() -> FilterExpr): FilterableQuery<E> {
        doFilter(baseTable, true, block)
        return this
    }

    override fun <REF : DbEntity<REF, *>> exclude(ref: RelToOne<E, REF>, block: FilterBuilder<REF>.() -> FilterExpr): FilterableQuery<E> {
        doFilter(baseTable.innerJoin(ref), true, block)
        return this
    }

    override fun <REF1 : DbEntity<REF1, *>, REF2 : DbEntity<REF2, *>> exclude(ref1: RelToOne<E, REF1>, ref2: RelToOne<REF1, REF2>, block: FilterBuilder<REF2>.() -> FilterExpr): FilterableQuery<E> {
        doFilter(baseTable.innerJoin(ref1).innerJoin(ref2), true, block)
        return this
    }
}

interface OrderableQuery<E: DbEntity<E, *>> {
    fun orderBy(order: Expr<in E, *>, ascending: Boolean = true): OrderableQuery<E>
    fun <R: DbEntity<R, *>>
        orderBy(ref: RelToSingle<E, R>, order: Expr<in R, *>, ascending: Boolean = true): OrderableQuery<E>
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: Expr<R2, *>, ascending: Boolean = true): OrderableQuery<E>

    fun orderBy(order: RowProp<E, *>, ascending: Boolean = true): OrderableQuery<E>
    fun <R: DbEntity<R, *>>
        orderBy(ref: RelToSingle<E, R>, order: RowProp<R, *>, ascending: Boolean = true): OrderableQuery<E>
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>>
        orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: RowProp<R2, *>, ascending: Boolean = true): OrderableQuery<E>

    fun offset(offset: Long): OrderableQuery<E>
    fun maxRowCount(maxRowCount: Int): OrderableQuery<E>
}

interface EntityQuery<E : DbEntity<E, *>>: FilterableQuery<E>, OrderableQuery<E> {
    val db: DbConn

    suspend fun run(selectForUpdate: Boolean = false): List<E>
    suspend fun countAll(): Long

    fun copy(includeOffsetAndLimit: Boolean = false): EntityQuery<E>
    fun copyAndRemapFilters(dstTable: TableInQuery<E>): FilterExpr?

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
            INITIAL -> queryState.startLoading({ loader.executeSelect(this, selectForUpdate) })
        }
    }

    override suspend fun countAll(): Long {
        return when (countState.state) {
            LOADED  -> countState.value
            LOADING -> suspendCoroutine(countState::addReceiver)
            INITIAL -> countState.startLoading({ loader.executeCount(this) })
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

        newQuery.filters = filters?.remap(remapper)
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

    override fun copyAndRemapFilters(dstTable: TableInQuery<E>): FilterExpr? {
        return filters?.let {
            val remapper = TableRemapper(dstTable.query)
            remapper.addExplicitMapping(baseTable, dstTable)
            return it.remap(remapper)
        }
    }

    override fun <OUT : Any> makeAggregateListQuery(factory: () -> OUT, queryBuilder: AggrListBuilder<OUT, E>.() -> Unit): AggrListQuery<OUT, E> {
        val query = table.makeAggregateListQuery(db, factory, {}) as AggrListImpl

        filters?.let { oldFilters ->
            val remapper = TableRemapper(query)
            remapper.addExplicitMapping(baseTable, query.baseTable)
            query.filter { oldFilters.remap(remapper) }
        }

        query.expand(queryBuilder)

        return query
    }

    override fun makeAggregateStreamQuery(queryBuilder: AggrStreamBuilder<E>.() -> Unit): AggrStreamQuery<E> {
        val query = table.makeAggregateStreamQuery(db, {}) as AggrStreamImpl

        filters?.let { oldFilters ->
            val remapper = TableRemapper(query)
            remapper.addExplicitMapping(baseTable, query.baseTable)
            query.filter { oldFilters.remap(remapper) }
        }

        query.expand(queryBuilder)

        return query
    }

    override fun toString(): String {
        return buildSelectQuery(this, false).getSql()
    }
}
