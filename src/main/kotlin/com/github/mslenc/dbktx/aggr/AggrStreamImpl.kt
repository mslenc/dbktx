package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.FilterableQuery
import com.github.mslenc.dbktx.crud.FilteringState
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.expr.SqlEmitter
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.sqltypes.SqlTypeDouble
import com.github.mslenc.dbktx.sqltypes.SqlTypeLong
import com.github.mslenc.dbktx.util.Sql

typealias RowCallback = (DbRow)->Unit

internal class AggrStreamImpl<E: DbEntity<E, *>>(table: DbTable<E, *>, val db: DbConn) : AggrQueryImpl<E>(table, db), AggrStreamQuery<E> {
    var executing = false

    val rowStartCallbacks = ArrayList<RowCallback>()
    val rowProcessCallbacks = ArrayList<RowCallback>()
    val rowEndCallbacks = ArrayList<RowCallback>()

    val selects = ArrayList<SqlEmitter>()
    val groupBy = ArrayList<SqlEmitter>()

    public override fun checkModifiable() {
        if (executing)
            throw IllegalStateException("Already querying")
    }

    override fun onRowStart(callback: (DbRow) -> Unit) {
        checkModifiable()
        rowStartCallbacks.add(callback)
    }

    override fun onRowEnd(callback: (DbRow) -> Unit) {
        checkModifiable()
        rowEndCallbacks.add(callback)
    }

    override suspend fun run(): Long {
        if (executing)
            throw IllegalStateException("Already querying")

        executing = true

        val allCallbacks = (rowStartCallbacks + rowProcessCallbacks + rowEndCallbacks).toTypedArray()
        val sql = buildQuery()

        return db.streamQuery(sql) { row ->
            for (i in 0 until allCallbacks.size) {
                allCallbacks[i].invoke(row)
            }
        }
    }

    internal fun buildQuery(): Sql {
        return Sql().apply {
            raw("SELECT ")
            for ((idx: Int, selectable: SqlEmitter) in selects.withIndex()) {
                if (idx > 0)
                    raw(", ")
                selectable.toSql(this, true)
            }

            FROM(baseTable)
            WHERE(filters)
            GROUP_BY(groupBy)

            if (!orderBy.isEmpty()) {
                +" ORDER BY "
                tuple(orderBy) {
                    +it.expr
                    if (!it.isAscending)
                        +" DESC"
                }
            }

            if (offset != null || maxRowCount != null) {
                +" LIMIT "
                this(maxRowCount ?: Integer.MAX_VALUE)
                +" OFFSET "
                this(offset ?: 0)
            }
        }
    }

    override fun expand(block: AggrStreamTopLevelBuilder<E>.() -> Unit): AggrStreamQuery<E> {
        checkModifiable()
        val builder = AggrStreamBuilderImpl(this, baseTable)
        builder.block()
        return this
    }

    override fun
    orderBy(order: Expr<in E, *>, ascending: Boolean): AggrStreamImpl<E> {
        addOrder(baseTable, order, ascending)
        return this
    }

    override fun <R : DbEntity<R, *>>
    orderBy(ref: RelToSingle<E, R>, order: Expr<in R, *>, ascending: Boolean): AggrStreamImpl<E> {
        addOrder(baseTable.leftJoin(ref), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: Expr<R2, *>, ascending: Boolean): AggrStreamImpl<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, order: Expr<R3, *>, ascending: Boolean): AggrStreamImpl<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, order: Expr<R4, *>, ascending: Boolean): AggrStreamImpl<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>, R5 : DbEntity<R5, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, ref5: RelToSingle<R4, R5>, order: Expr<R5, *>, ascending: Boolean): AggrStreamImpl<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4).leftJoin(ref5), order, ascending)
        return this
    }

    override fun
    orderBy(order: RowProp<E, *>, ascending: Boolean): AggrStreamImpl<E> {
        addOrder(baseTable, order, ascending)
        return this
    }

    override fun <R : DbEntity<R, *>>
    orderBy(ref: RelToSingle<E, R>, order: RowProp<R, *>, ascending: Boolean): AggrStreamImpl<E> {
        addOrder(baseTable.leftJoin(ref), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: RowProp<R2, *>, ascending: Boolean): AggrStreamImpl<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, order: RowProp<R3, *>, ascending: Boolean): AggrStreamImpl<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, order: RowProp<R4, *>, ascending: Boolean): AggrStreamImpl<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>, R3 : DbEntity<R3, *>, R4 : DbEntity<R4, *>, R5 : DbEntity<R5, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, ref3: RelToSingle<R2, R3>, ref4: RelToSingle<R3, R4>, ref5: RelToSingle<R4, R5>, order: RowProp<R5, *>, ascending: Boolean): AggrStreamImpl<E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2).leftJoin(ref3).leftJoin(ref4).leftJoin(ref5), order, ascending)
        return this
    }

    override fun offset(offset: Long): AggrStreamImpl<E> {
        setOffset(offset)
        return this
    }

    override fun maxRowCount(maxRowCount: Int): AggrStreamImpl<E> {
        setMaxRowCount(maxRowCount)
        return this
    }
}

internal class AggrStreamBuilderImpl<E: DbEntity<E, *>, CURR: DbEntity<CURR, *>>(val query: AggrStreamImpl<E>, val tableInQuery: TableInQuery<CURR>): AggrStreamTopLevelBuilder<CURR>, FilterableQuery<CURR> {
    override val baseTable: TableInQuery<CURR>
        get() = tableInQuery

    override fun require(filter: FilterExpr) {
        query.require(filter)
    }

    override fun filteringState(): FilteringState {
        return query.filteringState()
    }

    internal fun <T : Any, OUT: Any> addNullableAggregate(op: AggrOp, block: AggrExprBuilder<CURR>.() -> Expr<CURR, T>, sqlType: SqlType<OUT>): NullableAggrExpr<CURR, OUT> {
        query.checkModifiable()
        val builder = AggrExprBuilderImpl(tableInQuery)
        val expr = builder.block()
        val columnIndex = query.selects.size
        val aggrExpr = NullableAggrExprImpl<CURR, OUT>(op, expr, columnIndex, sqlType)
        query.selects.add(aggrExpr)
        return aggrExpr
    }

    internal fun <T: Any> addNullableAggregate(op: AggrOp, block: AggrExprBuilder<CURR>.() -> Expr<CURR, T>): NullableAggrExpr<CURR, T> {
        query.checkModifiable()
        val builder = AggrExprBuilderImpl(tableInQuery)
        val expr = builder.block()
        val columnIndex = query.selects.size
        val aggrExpr = NullableAggrExprImpl<CURR, T>(op, expr, columnIndex, expr.getSqlType())
        query.selects.add(aggrExpr)
        return aggrExpr
    }

    internal fun <T : Any, OUT: Any> addNonNullAggregate(op: AggrOp, block: AggrExprBuilder<CURR>.() -> Expr<CURR, T>, sqlType: SqlType<OUT>): NonNullAggrExpr<CURR, OUT> {
        query.checkModifiable()
        val builder = AggrExprBuilderImpl(tableInQuery)
        val expr = builder.block()
        val columnIndex = query.selects.size
        val aggrExpr = NonNullAggrExprImpl<CURR, OUT>(op, expr, columnIndex, sqlType)
        query.selects.add(aggrExpr)
        return aggrExpr
    }

    override fun <T : Any> sum(block: AggrExprBuilder<CURR>.() -> Expr<CURR, T>): NullableAggrExpr<CURR, T> {
        return addNullableAggregate(AggrOp.SUM, block)
    }

    override fun <T : Any> min(block: AggrExprBuilder<CURR>.() -> Expr<CURR, T>): NullableAggrExpr<CURR, T> {
        return addNullableAggregate(AggrOp.MIN, block)
    }

    override fun <T : Any> max(block: AggrExprBuilder<CURR>.() -> Expr<CURR, T>): NullableAggrExpr<CURR, T> {
        return addNullableAggregate(AggrOp.MAX, block)
    }

    override fun <T : Any> count(block: AggrExprBuilder<CURR>.() -> Expr<CURR, T>): NonNullAggrExpr<CURR, Long> {
        return addNonNullAggregate(AggrOp.COUNT, block, SqlTypeLong.INSTANCE_FOR_COUNT)
    }

    override fun <T : Any> countDistinct(block: AggrExprBuilder<CURR>.() -> Expr<CURR, T>): NonNullAggrExpr<CURR, Long> {
        return addNonNullAggregate(AggrOp.COUNT_DISTINCT, block, SqlTypeLong.INSTANCE_FOR_COUNT)
    }

    override fun <T : Any> average(block: AggrExprBuilder<CURR>.() -> Expr<CURR, T>): NullableAggrExpr<CURR, Double> {
        return addNullableAggregate(AggrOp.AVG, block, SqlTypeDouble.INSTANCE_FOR_AVG)
    }

    override fun <T: Any> NullableAggrExpr<CURR, T>.into(receiver: (T?) -> Unit) {
        query.checkModifiable()

        val aggrExpr = this as NullableAggrExprImpl

        query.rowProcessCallbacks.add { row ->
            receiver(aggrExpr.retrieveValue(row))
        }
    }

    override fun <T: Any> NonNullAggrExpr<CURR, T>.into(receiver: (T) -> Unit) {
        query.checkModifiable()

        val aggrExpr = this as NonNullAggrExprImpl

        query.rowProcessCallbacks.add { row ->
            receiver(aggrExpr.retrieveValue(row))
        }
    }

    override fun <T : Any> NullableColumn<CURR, T>.into(receiver: (T?) -> Unit) {
        query.checkModifiable()

        val column = this
        val sqlType = column.sqlType
        val boundColumn = column.bindForSelect(tableInQuery)
        val colIndex = query.selects.size
        query.selects.add(boundColumn)
        query.groupBy.add(boundColumn)

        query.rowProcessCallbacks.add { row ->
            val value = row.getValue(colIndex)
            if (value.isNull) {
                receiver.invoke(null)
            } else {
                receiver.invoke(sqlType.parseDbValue(value))
            }
        }
    }

    override fun <T : Any> NonNullColumn<CURR, T>.into(receiver: (T) -> Unit) {
        query.checkModifiable()

        val column = this
        val sqlType = column.sqlType
        val boundColumn = column.bindForSelect(tableInQuery)
        val colIndex = query.selects.size
        query.selects.add(boundColumn)
        query.groupBy.add(boundColumn)

        query.rowProcessCallbacks.add { row ->
            val value = row.getValue(colIndex)
            receiver.invoke(sqlType.parseDbValue(value))
        }
    }

    override fun onRowStart(block: (DbRow) -> Unit) {
        query.onRowStart(block)
    }

    override fun onRowEnd(block: (DbRow) -> Unit) {
        query.onRowEnd(block)
    }

    override fun <REF : DbEntity<REF, *>> innerJoin(ref: RelToOne<CURR, REF>, block: AggrStreamBuilder<REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = tableInQuery.innerJoin(ref)
        val subBuilder = AggrStreamBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> innerJoin(set: RelToMany<CURR, REF>, block: AggrStreamBuilder<REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = tableInQuery.innerJoin(set)
        val subBuilder = AggrStreamBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> leftJoin(ref: RelToOne<CURR, REF>, block: AggrStreamBuilder<REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = tableInQuery.leftJoin(ref)
        val subBuilder = AggrStreamBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> leftJoin(set: RelToMany<CURR, REF>, block: AggrStreamBuilder<REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = tableInQuery.leftJoin(set)
        val subBuilder = AggrStreamBuilderImpl(query, subTable)
        subBuilder.block()
    }
}