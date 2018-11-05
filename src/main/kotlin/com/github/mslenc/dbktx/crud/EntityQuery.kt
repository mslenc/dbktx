package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.buildSelectQuery
import com.github.mslenc.dbktx.util.EntityState.*
import com.github.mslenc.dbktx.expr.CompositeExpr
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.ExprBoolean
import com.github.mslenc.dbktx.expr.ExprBools
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

    fun filter(block: FilterBuilder<E>.() -> ExprBoolean): FilterableQuery<E>
    fun <REF: DbEntity<REF, *>>
        filter(ref: RelToOne<E, REF>, block: FilterBuilder<REF>.() -> ExprBoolean): FilterableQuery<E>
    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
        filter(ref1: RelToOne<E, REF1>, ref2: RelToOne<REF1, REF2>, block: FilterBuilder<REF2>.() -> ExprBoolean): FilterableQuery<E>


    fun exclude(block: FilterBuilder<E>.() -> ExprBoolean): FilterableQuery<E>
    fun <REF: DbEntity<REF, *>>
        exclude(ref: RelToOne<E, REF>, block: FilterBuilder<REF>.() -> ExprBoolean): FilterableQuery<E>
    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
        exclude(ref1: RelToOne<E, REF1>, ref2: RelToOne<REF1, REF2>, block: FilterBuilder<REF2>.() -> ExprBoolean): FilterableQuery<E>
}

internal abstract class FilterableQueryImpl<E: DbEntity<E, *>>(
        internal val table: DbTable<E, *>,
        internal val loader: DbConn) : QueryImpl(), FilterableQuery<E> {

    override val baseTable = makeBaseTable(table)

    protected abstract fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E>
    protected abstract fun checkModifiable()
    internal var filters: ExprBoolean? = null

    private fun <E: DbEntity<E, *>>
        doFilter(tableInQuery: TableInQuery<E>, negate: Boolean, block: FilterBuilder<E>.() -> ExprBoolean) {

        checkModifiable()

        val filterBuilder = TableInQueryBoundFilterBuilder(tableInQuery)
        val filter = filterBuilder.block()
        val finalFilter = if (negate) !filter else filter

        addFilter(finalFilter)
    }

    internal fun addFilter(filter: ExprBoolean) {
        val existing = this.filters

        if (existing != null) {
            this.filters = ExprBools.create(existing, ExprBools.Op.AND, filter)
        } else {
            this.filters = filter
        }
    }

    override fun filter(block: FilterBuilder<E>.() -> ExprBoolean): FilterableQuery<E> {
        doFilter(baseTable, false, block)
        return this
    }

    override fun <REF : DbEntity<REF, *>> filter(ref: RelToOne<E, REF>, block: FilterBuilder<REF>.() -> ExprBoolean): FilterableQuery<E> {
        doFilter(baseTable.innerJoin(ref), false, block)
        return this
    }

    override fun <REF1 : DbEntity<REF1, *>, REF2 : DbEntity<REF2, *>> filter(ref1: RelToOne<E, REF1>, ref2: RelToOne<REF1, REF2>, block: FilterBuilder<REF2>.() -> ExprBoolean): FilterableQuery<E> {
        doFilter(baseTable.innerJoin(ref1).innerJoin(ref2), false, block)
        return this
    }

    override fun exclude(block: FilterBuilder<E>.() -> ExprBoolean): FilterableQuery<E> {
        doFilter(baseTable, true, block)
        return this
    }

    override fun <REF : DbEntity<REF, *>> exclude(ref: RelToOne<E, REF>, block: FilterBuilder<REF>.() -> ExprBoolean): FilterableQuery<E> {
        doFilter(baseTable.innerJoin(ref), true, block)
        return this
    }

    override fun <REF1 : DbEntity<REF1, *>, REF2 : DbEntity<REF2, *>> exclude(ref1: RelToOne<E, REF1>, ref2: RelToOne<REF1, REF2>, block: FilterBuilder<REF2>.() -> ExprBoolean): FilterableQuery<E> {
        doFilter(baseTable.innerJoin(ref1).innerJoin(ref2), true, block)
        return this
    }
}

interface EntityQuery<E : DbEntity<E, *>>: FilterableQuery<E> {
    val db: DbConn

    suspend fun run(): List<E>
    suspend fun countAll(): Long

    fun orderBy(order: Expr<in E, *>, ascending: Boolean = true): EntityQuery<E>
    fun <R: DbEntity<R, *>>
        orderBy(ref: RelToOne<E, R>, order: Expr<in R, *>, ascending: Boolean = true): EntityQuery<E>
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>>
        orderBy(ref1: RelToOne<E, R1>, ref2: RelToOne<R1, R2>, order: Expr<R2, *>, ascending: Boolean = true): EntityQuery<E>

    fun orderBy(order: RowProp<E, *>, ascending: Boolean = true): EntityQuery<E>
    fun <R: DbEntity<R, *>>
        orderBy(ref: RelToOne<E, R>, order: RowProp<R, *>, ascending: Boolean = true): EntityQuery<E>
    fun <R1: DbEntity<R1, *>, R2: DbEntity<R2, *>>
        orderBy(ref1: RelToOne<E, R1>, ref2: RelToOne<R1, R2>, order: RowProp<R2, *>, ascending: Boolean = true): EntityQuery<E>

    fun offset(offset: Long): EntityQuery<E>
    fun maxRowCount(maxRowCount: Int): EntityQuery<E>

    fun copy(includeOffsetAndLimit: Boolean = false): EntityQuery<E>
    fun copyAndRemapFilters(dstTable: TableInQuery<E>): ExprBoolean?
}

class TableInQueryBoundFilterBuilder<E: DbEntity<E, *>>(val table: TableInQuery<E>) : FilterBuilder<E> {
    override fun currentTable(): TableInQuery<E> {
        return table
    }

    override fun <T: Any> bind(prop: RowProp<E, T>): Expr<E, T> {
        return prop.bindForSelect(table)
    }
}

internal class EntityQueryImpl<E : DbEntity<E, *>>(
        table: DbTable<E, *>,
        loader: DbConn)
    : FilterableQueryImpl<E>(table, loader), EntityQuery<E> {

    override fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E> {
        return BaseTableInQuery(this, table)
    }

    override val db: DbConn
        get() = loader

    private var  _orderBy: ArrayList<OrderSpec<*>>? = null
    internal val orderBy: List<OrderSpec<*>>
        get() {
            return _orderBy ?: emptyList()
        }

    internal var offset: Long? = null
    internal var maxRowCount: Int? = null

    private val queryState = DelayedLoadState<List<E>>()
    private val countState = DelayedLoadState<Long>()

    override suspend fun run(): List<E> {
        return when (queryState.state) {
            LOADED  -> queryState.value
            LOADING -> suspendCoroutine(queryState::addReceiver)
            INITIAL -> queryState.startLoading({ loader.executeSelect(this) })
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
    orderBy(order: Expr<in E, *>, ascending: Boolean): EntityQueryImpl<E> {
        addOrder(baseTable, order, ascending)
        return this
    }

    override fun <R : DbEntity<R, *>>
    orderBy(ref: RelToOne<E, R>, order: Expr<in R, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable.leftJoin(ref), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>>
    orderBy(ref1: RelToOne<E, R1>, ref2: RelToOne<R1, R2>, order: Expr<R2, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2), order, ascending)
        return this
    }

    override fun
    orderBy(order: RowProp<E, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable, order, ascending)
        return this
    }

    override fun <R : DbEntity<R, *>>
    orderBy(ref: RelToOne<E, R>, order: RowProp<R, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable.leftJoin(ref), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>>
    orderBy(ref1: RelToOne<E, R1>, ref2: RelToOne<R1, R2>, order: RowProp<R2, *>, ascending: Boolean): EntityQuery<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2), order, ascending)
        return this
    }

    private fun <R: DbEntity<R, *>>
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

    private fun <R: DbEntity<R, *>>
    addOrder(table: TableInQuery<R>, order: RowProp<R, *>, ascending: Boolean) {
        addOrder(table, order.bindForSelect(table), ascending)
    }

    override fun offset(offset: Long): EntityQuery<E> {
        checkModifiable()

        if (offset < 0)
            throw IllegalArgumentException("offset < 0")

        this.offset = offset

        return this
    }

    override fun maxRowCount(maxRowCount: Int): EntityQuery<E> {
        checkModifiable()

        if (maxRowCount < 0)
            throw IllegalArgumentException("maxRowCount < 0")

        this.maxRowCount = maxRowCount

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

    override fun copyAndRemapFilters(dstTable: TableInQuery<E>): ExprBoolean? {
        return filters?.let {
            val remapper = TableRemapper(dstTable.query)
            remapper.addExplicitMapping(baseTable, dstTable)
            return it.remap(remapper)
        }
    }

    override fun toString(): String {
        return buildSelectQuery(this).getSql()
    }
}
