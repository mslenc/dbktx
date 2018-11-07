package com.github.mslenc.dbktx.aggr

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.*
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.SqlEmitter
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import com.github.mslenc.dbktx.schema.RelToOne
import com.github.mslenc.dbktx.schema.RowProp
import com.github.mslenc.dbktx.util.DelayedLoadState
import com.github.mslenc.dbktx.util.EntityState.LOADED
import com.github.mslenc.dbktx.util.EntityState.LOADING
import com.github.mslenc.dbktx.util.EntityState.INITIAL
import kotlin.coroutines.suspendCoroutine

interface AggregateQuery<E : DbEntity<E, *>> : FilterableQuery<E>, OrderableQuery<E> {
    fun expand(builder: AggregateBuilder<E>.()->Unit): AggregateQuery<E>

    suspend fun run(): List<AggregateRow>
}


internal class AggregateQueryImpl<E : DbEntity<E, *>>(table: DbTable<E, *>, db: DbConn) : OrderableFilterableQueryImpl<E>(table, db), OrderableQuery<E>, AggregateQuery<E> {
    override fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E> {
        return BaseTableInQuery(this, table)
    }

    private val queryState = DelayedLoadState<List<AggregateRow>>()

    private var selects = ArrayList<SqlEmitter>()
    private var groupBy = ArrayList<Expr<*, *>>()
    private var bindings = ArrayList<BoundAggregateExpr<*>>()
    private var sourceBindings = HashMap<Any, BoundAggregateExpr<*>>()

    override fun expand(builder: AggregateBuilder<E>.() -> Unit): AggregateQuery<E> {
        AggregateBuilderImpl(this, baseTable).builder()
        return this
    }

    internal fun <Z: DbEntity<Z, *>, T : Any> addSelectAndGroupBy(boundColumn: BoundColumnForSelect<Z, T>): BoundAggregateExpr<T> {
        val binding = BoundColumnExpr(boundColumn, selects.size)

        bindings.add(binding)
        selects.add(binding)
        groupBy.add(boundColumn)

        if (!sourceBindings.containsKey(boundColumn.column))
            sourceBindings[boundColumn.column] = binding

        return binding
    }

    internal fun <Z : DbEntity<Z, *>, T : Any> addSelect(expr: AggregateExpr<Z, T>, table: TableInQuery<Z>): BoundAggregateExpr<T> {
        val binding = expr.bind(table, selects.size)

        bindings.add(binding)
        selects.add(binding)

        if (!sourceBindings.containsKey(expr))
            sourceBindings[expr] = binding

        return binding
    }

    override suspend fun run(): List<AggregateRow> {
        return when (queryState.state) {
            LOADED  -> queryState.value
            LOADING -> suspendCoroutine(queryState::addReceiver)
            INITIAL -> queryState.startLoading({ loader.executeSelect(this) })
        }
    }

    override fun checkModifiable() {
        if (queryState.state !== INITIAL)
            throw IllegalStateException("Already querying")
    }

    override fun
    orderBy(order: Expr<in E, *>, ascending: Boolean): AggregateQuery<E> {
        addOrder(baseTable, order, ascending)
        return this
    }

    override fun <R : DbEntity<R, *>>
    orderBy(ref: RelToOne<E, R>, order: Expr<in R, *>, ascending: Boolean): AggregateQuery<E> {
        addOrder(baseTable.leftJoin(ref), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>>
    orderBy(ref1: RelToOne<E, R1>, ref2: RelToOne<R1, R2>, order: Expr<R2, *>, ascending: Boolean): AggregateQuery<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2), order, ascending)
        return this
    }

    override fun
    orderBy(order: RowProp<E, *>, ascending: Boolean): AggregateQuery<E> {
        addOrder(baseTable, order, ascending)
        return this
    }

    override fun <R : DbEntity<R, *>>
    orderBy(ref: RelToOne<E, R>, order: RowProp<R, *>, ascending: Boolean): AggregateQuery<E> {
        addOrder(baseTable.leftJoin(ref), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>>
    orderBy(ref1: RelToOne<E, R1>, ref2: RelToOne<R1, R2>, order: RowProp<R2, *>, ascending: Boolean): AggregateQuery<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2), order, ascending)
        return this
    }

    override fun offset(offset: Long): AggregateQuery<E> {
        setOffset(offset)
        return this
    }

    override fun maxRowCount(maxRowCount: Int): AggregateQuery<E> {
        setMaxRowCount(maxRowCount)
        return this
    }
}