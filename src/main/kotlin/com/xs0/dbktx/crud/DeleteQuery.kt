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

interface DeleteQuery<E : DbEntity<E, *>>: FilterableQuery<E> {

    suspend fun executeDelete()

}


internal class DeleteQueryImpl<E : DbEntity<E, ID>, ID: Any>(
        table: DbTable<E, ID>,
        loader: DbConn)
    : FilterableQueryImpl<E, ID>(table, loader), DeleteQuery<E> {

    override suspend fun executeDelete() {
        loader.delete(table, this.filters)
    }


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

    override fun checkModifiable() {
        if (queryState.state !== INITIAL || countState.state !== INITIAL)
            throw IllegalStateException("Already querying")
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
