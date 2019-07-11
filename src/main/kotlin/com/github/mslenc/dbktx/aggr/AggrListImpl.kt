package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.FilterBuilder
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.crud.TableInQueryBoundFilterBuilder
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.expr.SqlEmitter
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.sqltypes.SqlTypeDouble
import com.github.mslenc.dbktx.sqltypes.SqlTypeLong
import com.github.mslenc.dbktx.util.Sql
import kotlin.reflect.KMutableProperty1

internal typealias FieldSetter<OUT> = (DbRow, OUT)->Unit

internal class AggrListImpl<OUT: Any, E: DbEntity<E, *>>(table: DbTable<E, *>, val db: DbConn, val outFactory: ()->OUT) : AggrQueryImpl<E>(table, db), AggrListQuery<OUT, E> {
    var executing = false

    val fieldSetters = ArrayList<FieldSetter<OUT>>()

    val selects = ArrayList<SqlEmitter>()
    val groupBy = ArrayList<SqlEmitter>()

    public override fun checkModifiable() {
        if (executing)
            throw IllegalStateException("Already querying")
    }

    override suspend fun run(): List<OUT> {
        if (executing)
            throw IllegalStateException("Already querying")

        executing = true

        val setters = fieldSetters.toTypedArray()
        val sql = buildQuery()

        val result = ArrayList<OUT>()
        db.streamQuery(sql) { row ->
            val out = outFactory()
            for (i in 0 until setters.size) {
                setters[i].invoke(row, out)
            }
            result.add(out)
        }

        return result
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

    override fun expand(block: AggrListBuilder<OUT, E>.() -> Unit): AggrListQuery<OUT, E> {
        checkModifiable()
        val builder = AggrListBuilderImpl(this, baseTable)
        builder.block()
        return this
    }

    override fun
    orderBy(order: Expr<in E, *>, ascending: Boolean): AggrListQuery<OUT, E> {
        addOrder(baseTable, order, ascending)
        return this
    }

    override fun <R : DbEntity<R, *>>
    orderBy(ref: RelToSingle<E, R>, order: Expr<in R, *>, ascending: Boolean): AggrListQuery<OUT, E> {
        addOrder(baseTable.leftJoin(ref), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: Expr<R2, *>, ascending: Boolean): AggrListQuery<OUT, E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2), order, ascending)
        return this
    }

    override fun
    orderBy(order: RowProp<E, *>, ascending: Boolean): AggrListQuery<OUT, E> {
        addOrder(baseTable, order, ascending)
        return this
    }

    override fun <R : DbEntity<R, *>>
    orderBy(ref: RelToSingle<E, R>, order: RowProp<R, *>, ascending: Boolean): AggrListQuery<OUT, E> {
        addOrder(baseTable.leftJoin(ref), order, ascending)
        return this
    }

    override fun <R1 : DbEntity<R1, *>, R2 : DbEntity<R2, *>>
    orderBy(ref1: RelToSingle<E, R1>, ref2: RelToSingle<R1, R2>, order: RowProp<R2, *>, ascending: Boolean): AggrListQuery<OUT, E> {
        addOrder(baseTable.leftJoin(ref1).leftJoin(ref2), order, ascending)
        return this
    }

    override fun offset(offset: Long): AggrListQuery<OUT, E> {
        setOffset(offset)
        return this
    }

    override fun maxRowCount(maxRowCount: Int): AggrListQuery<OUT, E> {
        setMaxRowCount(maxRowCount)
        return this
    }
}

internal class AggrListBuilderImpl<OUT: Any, E: DbEntity<E, *>, CURR: DbEntity<CURR, *>>(val query: AggrListImpl<OUT, E>, val tableInQuery: TableInQuery<CURR>): AggrListBuilder<OUT, CURR> {
    internal fun <T : Any, RES: Any> addNullableAggregate(op: AggrOp, block: AggrExprBuilder<CURR>.() -> Expr<CURR, T>, sqlType: SqlType<RES>): NullableAggrExpr<CURR, RES> {
        query.checkModifiable()
        val builder = AggrExprBuilderImpl(tableInQuery)
        val expr = builder.block()
        val columnIndex = query.selects.size
        val aggrExpr = NullableAggrExprImpl<CURR, RES>(op, expr, columnIndex, sqlType)
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

    internal fun <T : Any, RES: Any> addNonNullAggregate(op: AggrOp, block: AggrExprBuilder<CURR>.() -> Expr<CURR, T>, sqlType: SqlType<RES>): NonNullAggrExpr<CURR, RES> {
        query.checkModifiable()
        val builder = AggrExprBuilderImpl(tableInQuery)
        val expr = builder.block()
        val columnIndex = query.selects.size
        val aggrExpr = NonNullAggrExprImpl<CURR, RES>(op, expr, columnIndex, sqlType)
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

    override fun <T: Any> KMutableProperty1<OUT, T?>.becomes(expr: NullableAggrExpr<CURR, T>) {
        query.checkModifiable()

        val prop = this
        expr as NullableAggrExprImpl

        query.fieldSetters.add { row, out ->
            prop.set(out, expr.retrieveValue(row))
        }
    }

    override fun <T: Any> KMutableProperty1<OUT, T>.becomes(expr: NonNullAggrExpr<CURR, T>) {
        query.checkModifiable()

        val prop = this
        expr as NonNullAggrExprImpl

        query.fieldSetters.add { row, out ->
            prop.set(out, expr.retrieveValue(row))
        }
    }

    override fun <T : Any> KMutableProperty1<OUT, T>.becomes(column: NonNullColumn<CURR, T>) {
        query.checkModifiable()

        val prop = this
        val sqlType = column.sqlType
        val boundColumn = column.bindForSelect(tableInQuery)
        val colIndex = query.selects.size
        query.selects.add(boundColumn)
        query.groupBy.add(boundColumn)

        query.fieldSetters.add { row, out ->
            prop.set(out, sqlType.parseDbValue(row.getValue(colIndex)))
        }
    }

    override fun <T : Any> KMutableProperty1<OUT, T?>.becomes(column: NullableColumn<CURR, T>) {
        query.checkModifiable()

        val prop = this
        val sqlType = column.sqlType
        val boundColumn = column.bindForSelect(tableInQuery)
        val colIndex = query.selects.size
        query.selects.add(boundColumn)
        query.groupBy.add(boundColumn)

        query.fieldSetters.add { row, out ->
            val value = row.getValue(colIndex)
            if (value.isNull) {
                prop.set(out, null)
            } else {
                prop.set(out, sqlType.parseDbValue(value))
            }
        }
    }

    override fun <REF : DbEntity<REF, *>> innerJoin(ref: RelToOne<CURR, REF>, block: AggrListBuilder<OUT, REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = tableInQuery.innerJoin(ref)
        val subBuilder = AggrListBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> innerJoin(set: RelToMany<CURR, REF>, block: AggrListBuilder<OUT, REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = tableInQuery.innerJoin(set)
        val subBuilder = AggrListBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> leftJoin(ref: RelToOne<CURR, REF>, block: AggrListBuilder<OUT, REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = tableInQuery.leftJoin(ref)
        val subBuilder = AggrListBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> leftJoin(set: RelToMany<CURR, REF>, block: AggrListBuilder<OUT, REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = tableInQuery.leftJoin(set)
        val subBuilder = AggrListBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun filter(block: FilterBuilder<CURR>.() -> FilterExpr) {
        val filterBuilder = TableInQueryBoundFilterBuilder(tableInQuery)
        val filterExpr = filterBuilder.block()
        query.addFilter(filterExpr)
    }

    override fun exclude(block: FilterBuilder<CURR>.() -> FilterExpr) {
        val filterBuilder = TableInQueryBoundFilterBuilder(tableInQuery)
        val filterExpr = filterBuilder.block()
        query.addFilter(filterExpr.not())
    }
}