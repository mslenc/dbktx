package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.BaseTableInQuery
import com.github.mslenc.dbktx.crud.FilterableQueryImpl
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.SqlEmitter
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.DelayedLoadState
import com.github.mslenc.dbktx.util.EntityState.LOADED
import com.github.mslenc.dbktx.util.EntityState.LOADING
import com.github.mslenc.dbktx.util.EntityState.INITIAL
import com.github.mslenc.dbktx.util.OrderSpec
import kotlin.coroutines.suspendCoroutine


internal class AggregateQueryImpl<E : DbEntity<E, *>>(table: DbTable<E, *>, db: DbConn) : FilterableQueryImpl<E>(table, db) {
    override fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E> {
        return BaseTableInQuery(this, table)
    }

    private var  _orderBy: ArrayList<OrderSpec<*>>? = null
    internal val orderBy: List<OrderSpec<*>>
        get() {
            return _orderBy ?: emptyList()
        }

    internal var offset: Long? = null
    internal var maxRowCount: Int? = null

    private val queryState = DelayedLoadState<List<DbRow>>()

    private var selects = ArrayList<SqlEmitter>()
    private var groupBy = ArrayList<Expr<*, *>>()
    private var bindings = ArrayList<BoundAggregateExpr<*>>()
    private var sourceBindings = HashMap<Any, BoundAggregateExpr<*>>()

    internal fun <Z: DbEntity<Z, *>, T : Any> addSelectAndGroupBy(expr: Expr<Z, T>, type: SqlType<T>): BoundAggregateExpr<T> {
        val binding = AggregateBinding<Z, T>(type, bindings.size)

        bindings.add(binding)
        selects.add(expr)
        groupBy.add(expr)

        if (!sourceBindings.containsKey(expr))
            sourceBindings[expr] = binding

        return binding
    }

    internal fun <Z : DbEntity<Z, *>, T : Any> addSelect(expr: AggregateExpr<Z, T>, table: TableInQuery<Z>): AggregateBinding<T> {
        val binding = expr.bind(table, bindings.size)

        bindings.add(binding)
        selects.add(binding)

        if (!sourceBindings.containsKey(expr))
            sourceBindings[expr] = binding

        return binding
    }

    suspend fun run(): List<DbRow> {
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
}