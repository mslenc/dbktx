package com.xs0.dbktx.crud

import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.util.EntityState.*
import com.xs0.dbktx.expr.CompositeExpr
import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable
import com.xs0.dbktx.util.DelayedLoadState
import com.xs0.dbktx.util.OrderSpec
import java.util.ArrayList
import kotlin.coroutines.experimental.suspendCoroutine

interface EntityQuery<E : DbEntity<E, *>> {
    suspend fun run(): List<E>
    suspend fun countAll(): Long

    fun filter(filter: ExprBoolean<E>): EntityQuery<E>
    fun exclude(exclude: ExprBoolean<E>): EntityQuery<E>

    fun orderBy(order: Expr<in E, *>, ascending: Boolean = true): EntityQuery<E>

    fun offset(offset: Long): EntityQuery<E>
    fun maxRowCount(maxRowCount: Int): EntityQuery<E>
}

internal class EntityQueryImpl<E : DbEntity<E, ID>, ID: Any>(
        internal val table: DbTable<E, ID>,
        internal val loader: DbConn)
    : EntityQuery<E> {

    internal var filter: ExprBoolean<E>? = null
    private var  _orderBy: ArrayList<OrderSpec<E>>? = null
    internal val orderBy: List<OrderSpec<E>>
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
            INITIAL -> countState.startLoading({ loader.count(table, filter) })
        }
    }

    private fun checkModifiable() {
        if (queryState.state !== INITIAL || countState.state !== INITIAL)
            throw IllegalStateException("Already querying")
    }

    override fun filter(filter: ExprBoolean<E>): EntityQueryImpl<E, ID> {
        checkModifiable()
        val existing = this.filter

        if (existing != null) {
            this.filter = existing and filter
        } else {
            this.filter = filter
        }
        return this
    }

    override fun exclude(exclude: ExprBoolean<E>): EntityQueryImpl<E, ID> {
        return filter(exclude.not())
    }

    override fun orderBy(order: Expr<in E, *>, ascending: Boolean): EntityQueryImpl<E, ID> {
        checkModifiable()

        if (order.isComposite) {
            val comp = order as CompositeExpr
            var i = 0
            val n = comp.numParts
            while (i < n) {
                orderBy(comp.getPart(i), ascending)
                i++
            }
        } else {
            if (_orderBy == null)
                _orderBy = ArrayList()

            _orderBy?.add(OrderSpec(order, ascending))
        }

        return this
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
