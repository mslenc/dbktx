package com.xs0.dbktx.crud

import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.util.EntityState.*
import com.xs0.dbktx.expr.CompositeExpr
import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.expr.ExprBools
import com.xs0.dbktx.schema.*
import com.xs0.dbktx.util.DelayedLoadState
import com.xs0.dbktx.util.OrderSpec
import java.util.ArrayList
import kotlin.coroutines.experimental.suspendCoroutine

interface Query {

}

internal open class QueryImpl {
    private val alias2table = LinkedHashMap<String, TableInQuery<*>>()

    fun isTableAliasTaken(tableAlias: String): Boolean {
        return alias2table.containsKey(tableAlias)
    }

    fun registerTableInQuery(tableInQuery: TableInQuery<*>) {
        alias2table.put(tableInQuery.tableAlias, tableInQuery)
    }
}

interface EntityQuery<E : DbEntity<E, *>>: Query {
    suspend fun run(): List<E>
    suspend fun countAll(): Long

    fun filter(block: FilterBuilder<E>.() -> ExprBoolean): EntityQuery<E>
    fun <REF: DbEntity<REF, *>>
        filter(ref: RelToOne<E, REF>, block: FilterBuilder<REF>.() -> ExprBoolean): EntityQuery<E>
    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
        filter(ref1: RelToOne<E, REF1>, ref2: RelToOne<REF1, REF2>, block: FilterBuilder<REF2>.() -> ExprBoolean): EntityQuery<E>


    fun exclude(block: FilterBuilder<E>.() -> ExprBoolean): EntityQuery<E>
    fun <REF: DbEntity<REF, *>>
        exclude(ref: RelToOne<E, REF>, block: FilterBuilder<REF>.() -> ExprBoolean): EntityQuery<E>
    fun <REF1: DbEntity<REF1, *>, REF2: DbEntity<REF2, *>>
        exclude(ref1: RelToOne<E, REF1>, ref2: RelToOne<REF1, REF2>, block: FilterBuilder<REF2>.() -> ExprBoolean): EntityQuery<E>


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
}

class TableInQueryBoundFilterBuilder<E: DbEntity<E, *>>(val table: TableInQuery<E>) : FilterBuilder<E> {
    override fun <T: Any> bind(prop: RowProp<E, T>): Expr<E, T> {
        return prop.bindForSelect(table)
    }
}

internal class EntityQueryImpl<E : DbEntity<E, ID>, ID: Any>(
        internal val table: DbTable<E, ID>,
        internal val loader: DbConn)
    : QueryImpl(), EntityQuery<E> {

    private val baseTable = BaseTableInQuery(this, table)

    internal var filters: ExprBoolean? = null

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
            INITIAL -> queryState.startLoading({ loader.query(this) })
        }
    }

    override suspend fun countAll(): Long {
        return when (countState.state) {
            LOADED  -> countState.value
            LOADING -> suspendCoroutine(countState::addReceiver)
            INITIAL -> countState.startLoading({ loader.count(table, filters) })
        }
    }

    private fun checkModifiable() {
        if (queryState.state !== INITIAL || countState.state !== INITIAL)
            throw IllegalStateException("Already querying")
    }

    private fun <E: DbEntity<E, *>>
        doFilter(tableInQuery: TableInQuery<E>, negate: Boolean, block: FilterBuilder<E>.() -> ExprBoolean) {

        checkModifiable()

        val filterBuilder = TableInQueryBoundFilterBuilder(tableInQuery)
        val filter = filterBuilder.block()
        val finalFilter = if (negate) !filter else filter

        val existing = this.filters

        if (existing != null) {
            this.filters = ExprBools.create(existing, ExprBools.Op.AND, finalFilter)
        } else {
            this.filters = finalFilter
        }
    }

    override fun filter(block: FilterBuilder<E>.() -> ExprBoolean): EntityQuery<E> {
        doFilter(baseTable, false, block)
        return this
    }

    override fun <REF : DbEntity<REF, *>> filter(ref: RelToOne<E, REF>, block: FilterBuilder<REF>.() -> ExprBoolean): EntityQuery<E> {
        doFilter(baseTable.innerJoin(ref), false, block)
        return this
    }

    override fun <REF1 : DbEntity<REF1, *>, REF2 : DbEntity<REF2, *>> filter(ref1: RelToOne<E, REF1>, ref2: RelToOne<REF1, REF2>, block: FilterBuilder<REF2>.() -> ExprBoolean): EntityQuery<E> {
        doFilter(baseTable.innerJoin(ref1).innerJoin(ref2), false, block)
        return this
    }

    override fun exclude(block: FilterBuilder<E>.() -> ExprBoolean): EntityQuery<E> {
        doFilter(baseTable, true, block)
        return this
    }

    override fun <REF : DbEntity<REF, *>> exclude(ref: RelToOne<E, REF>, block: FilterBuilder<REF>.() -> ExprBoolean): EntityQuery<E> {
        doFilter(baseTable.innerJoin(ref), true, block)
        return this
    }

    override fun <REF1 : DbEntity<REF1, *>, REF2 : DbEntity<REF2, *>> exclude(ref1: RelToOne<E, REF1>, ref2: RelToOne<REF1, REF2>, block: FilterBuilder<REF2>.() -> ExprBoolean): EntityQuery<E> {
        doFilter(baseTable.innerJoin(ref1).innerJoin(ref2), true, block)
        return this
    }

    override fun
    orderBy(order: Expr<in E, *>, ascending: Boolean): EntityQueryImpl<E, ID> {
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
    orderBy(order: RowProp<E, *>, ascending: Boolean): EntityQueryImpl<E, ID> {
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

    override fun offset(offset: Long): EntityQueryImpl<E, ID> {
        checkModifiable()

        if (offset < 0)
            throw IllegalArgumentException("offset < 0")

        this.offset = offset

        return this
    }

    override fun maxRowCount(maxRowCount: Int): EntityQueryImpl<E, ID> {
        checkModifiable()

        if (maxRowCount < 0)
            throw IllegalArgumentException("maxRowCount < 0")

        this.maxRowCount = maxRowCount

        return this
    }
}
