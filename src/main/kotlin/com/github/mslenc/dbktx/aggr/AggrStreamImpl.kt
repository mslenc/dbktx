package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.*
import com.github.mslenc.dbktx.crud.BaseTableInQuery
import com.github.mslenc.dbktx.crud.OrderableFilterableQueryImpl
import com.github.mslenc.dbktx.expr.*
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.sqltypes.SqlTypeDouble
import com.github.mslenc.dbktx.sqltypes.SqlTypeLong
import com.github.mslenc.dbktx.util.Sql

typealias RowCallback = (DbRow)->Unit

internal class AggrStreamImpl<E: DbEntity<E, *>>(table: DbTable<E, *>, db: DbConn) : OrderableFilterableQueryImpl<E>(table, db), AggrStreamQuery<E> {
    override val aggregatesAllowed: Boolean
        get() = true

    var executing = false

    val rowStartCallbacks = ArrayList<RowCallback>()
    val rowProcessCallbacks = ArrayList<RowCallback>()
    val rowEndCallbacks = ArrayList<RowCallback>()

    val selects = ArrayList<SqlEmitter>()
    val groupBy = ArrayList<SqlEmitter>()

    override fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E> {
        return BaseTableInQuery(this, table)
    }

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

    override suspend fun execute(): Long {
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
        return Sql(db.dbType).apply {
            raw("SELECT ")
            for ((idx: Int, selectable: SqlEmitter) in selects.withIndex()) {
                if (idx > 0)
                    raw(", ")
                selectable.toSql(this, true)
            }

            FROM(table)
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

    override fun expand(block: AggrStreamTopLevelBuilder<E>.() -> Unit) {
        checkModifiable()
        AggrStreamBuilderImpl(this, table).block()
    }
}

internal class AggrStreamBuilderImpl<E: DbEntity<E, *>, CURR: DbEntity<CURR, *>>(val query: AggrStreamImpl<E>, override val table: TableInQuery<CURR>): AggrStreamTopLevelBuilder<CURR>, FilterableQuery<CURR> {
    override val aggregatesAllowed: Boolean
        get() = true

    override fun require(filter: Expr<Boolean>) {
        query.require(filter)
    }

    override fun include(filter: Expr<Boolean>) {
        query.include(filter)
    }

    override fun filteringState(): FilteringState {
        return query.filteringState()
    }

    override fun checkpoint(): FilterCheckpoint {
        return query.checkpoint()
    }

    override fun <T : Any> sum(block: ExprBuilder<CURR>.() -> Expr<T>): NullableAggrExpr<T> {
        val inner = table.newExprBuilder().block()
        return AggrExprImpl(AggrExprOp.SUM, inner, inner.sqlType)
    }

    override fun <T : Comparable<T>> min(block: ExprBuilder<CURR>.() -> Expr<T>): NullableAggrExpr<T> {
        val inner = table.newExprBuilder().block()
        return AggrExprImpl(AggrExprOp.MIN, inner, inner.sqlType)
    }

    override fun <T : Comparable<T>> max(block: ExprBuilder<CURR>.() -> Expr<T>): NullableAggrExpr<T> {
        val inner = table.newExprBuilder().block()
        return AggrExprImpl(AggrExprOp.MAX, inner, inner.sqlType)
    }

    override fun <T : Number> average(block: ExprBuilder<CURR>.() -> Expr<T>): NullableAggrExpr<Double> {
        val inner = table.newExprBuilder().block()
        return AggrExprImpl(AggrExprOp.AVG, inner, SqlTypeDouble.INSTANCE_FOR_AVG)
    }

    override fun <T : Any> count(block: ExprBuilder<CURR>.() -> Expr<T>): NonNullAggrExpr<Long> {
        val inner = table.newExprBuilder().block()
        return CountExpr(CountExprOp.COUNT, inner, SqlTypeLong.INSTANCE_FOR_COUNT)
    }

    override fun <T : Any> countDistinct(block: ExprBuilder<CURR>.() -> Expr<T>): NonNullAggrExpr<Long> {
        val inner = table.newExprBuilder().block()
        return CountExpr(CountExprOp.COUNT_DISTINCT, inner, SqlTypeLong.INSTANCE_FOR_COUNT)
    }

    override fun <T : Any> expr(block: AggrExprBuilder<CURR>.() -> Expr<T>): Expr<T> {
        return table.newExprBuilder().block()
    }

    override fun <T : Any> NonNullAggrExpr<T>.intoNN(receiver: (T) -> Unit) {
        query.checkModifiable()

        val columnIndex = query.selects.size
        val sqlType = this.sqlType

        query.selects.add(this)
        query.rowProcessCallbacks.add { row ->
            val dbValue = row.getValue(columnIndex)
            receiver(sqlType.parseDbValue(dbValue))
        }
    }

    override fun <T: Any> Expr<T>.into(receiver: (T?) -> Unit) {
        query.checkModifiable()

        val columnIndex = query.selects.size
        val sqlType = this.sqlType

        query.selects.add(this)
        query.rowProcessCallbacks.add { row ->
            val dbValue = row.getValue(columnIndex)
            when {
                dbValue.isNull -> receiver(null)
                else -> receiver(sqlType.parseDbValue(dbValue))
            }
        }
    }

    private fun <T : Any> addColumn(column: Column<CURR, T>): Int {
        query.checkModifiable()

        val boundColumn = column.bindForSelect(this@AggrStreamBuilderImpl.table)
        val colIndex = query.selects.size
        query.selects.add(boundColumn)
        query.groupBy.add(boundColumn)

        return colIndex
    }

    override fun <T : Any> NullableColumn<CURR, T>.into(receiver: (T?) -> Unit) {
        val colIndex = addColumn(this)
        val sqlType = this.sqlType

        query.rowProcessCallbacks.add { row ->
            val value = row.getValue(colIndex)
            if (value.isNull) {
                receiver.invoke(null)
            } else {
                receiver.invoke(sqlType.parseDbValue(value))
            }
        }
    }

    override fun <T : Any> NonNullColumn<CURR, T>.intoNN(receiver: (T) -> Unit) {
        val colIndex = addColumn(this)
        val sqlType = this.sqlType

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

    override fun <REF : DbEntity<REF, *>> innerJoin(ref: RelToSingle<CURR, REF>, block: AggrStreamBuilder<REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = table.innerJoin(ref)
        val subBuilder = AggrStreamBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> innerJoin(set: RelToMany<CURR, REF>, block: AggrStreamBuilder<REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = table.innerJoin(set)
        val subBuilder = AggrStreamBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> leftJoin(ref: RelToSingle<CURR, REF>, block: AggrStreamBuilder<REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = table.leftJoin(ref)
        val subBuilder = AggrStreamBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> leftJoin(set: RelToMany<CURR, REF>, block: AggrStreamBuilder<REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = table.leftJoin(set)
        val subBuilder = AggrStreamBuilderImpl(query, subTable)
        subBuilder.block()
    }
}