package com.github.mslenc.dbktx.aggr

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.FilterableQuery
import com.github.mslenc.dbktx.crud.FilteringState
import com.github.mslenc.dbktx.crud.OrderableFilterableQueryImpl
import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.ExprNull
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.expr.SqlEmitter
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.sqltypes.SqlTypeDouble
import com.github.mslenc.dbktx.sqltypes.SqlTypeLong
import com.github.mslenc.dbktx.util.Sql

internal class AggrInsertSelectQueryImpl<OUT: DbEntity<OUT, *>, ROOT: DbEntity<ROOT, *>>(val outTable: DbTable<OUT, *>, queryRoot: DbTable<ROOT, *>, db: DbConn) : OrderableFilterableQueryImpl<ROOT>(queryRoot, db), AggrInsertSelectQuery<OUT, ROOT> {
    var executing = false

    val outColumns = ArrayList<Column<OUT, *>>()
    val selects = ArrayList<SqlEmitter>()
    val groupBy = ArrayList<SqlEmitter>()

    public override fun checkModifiable() {
        if (executing)
            throw IllegalStateException("Already querying")
    }

    override suspend fun execute(): Long {
        if (executing)
            throw IllegalStateException("Already querying")

        executing = true

        return db.executeInsertSelect(buildQuery(), outTable)
    }

    internal fun buildQuery(): Sql {
        return Sql(db.dbType).apply {
            raw("INSERT INTO ")(outTable).raw("(")
            for ((idx, col) in outColumns.withIndex()) {
                if (idx > 0)
                    raw(", ")
                raw(col.quotedFieldName)
            }
            raw(") SELECT ")
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

    override fun expand(block: AggrInsertSelectTopLevelBuilder<OUT, ROOT>.() -> Unit) {
        checkModifiable()
        val builder = AggrInsertSelectBuilderImpl(this, baseTable)
        builder.block()
    }
}

internal class AggrInsertSelectBuilderImpl<OUT: DbEntity<OUT, *>, ROOT: DbEntity<ROOT, *>, CURR: DbEntity<CURR, *>>(val query: AggrInsertSelectQueryImpl<OUT, ROOT>, val tableInQuery: TableInQuery<CURR>): AggrInsertSelectTopLevelBuilder<OUT, CURR>, FilterableQuery<CURR> {
    override val baseTable: TableInQuery<CURR>
        get() = tableInQuery

    override fun require(filter: FilterExpr) {
        query.require(filter)
    }

    override fun filteringState(): FilteringState {
        return query.filteringState()
    }

    internal fun <T : Any, OUT: Any> addNullableAggregate(op: AggrOp, block: AggrExprBuilder<CURR>.() -> Expr<T>, sqlType: SqlType<OUT>): NullableAggrExpr<CURR, OUT> {
        query.checkModifiable()
        val builder = AggrExprBuilderImpl(tableInQuery)
        val expr = builder.block()
        val columnIndex = query.selects.size
        val aggrExpr = NullableAggrExprImpl<CURR, OUT>(op, expr, columnIndex, sqlType)
        query.selects.add(aggrExpr)
        return aggrExpr
    }

    internal fun <T: Any> addNullableAggregate(op: AggrOp, block: AggrExprBuilder<CURR>.() -> Expr<T>): NullableAggrExpr<CURR, T> {
        query.checkModifiable()
        val builder = AggrExprBuilderImpl(tableInQuery)
        val expr = builder.block()
        val columnIndex = query.selects.size
        val aggrExpr = NullableAggrExprImpl<CURR, T>(op, expr, columnIndex, expr.getSqlType())
        query.selects.add(aggrExpr)
        return aggrExpr
    }

    internal fun <T : Any, OUT: Any> addNonNullAggregate(op: AggrOp, block: AggrExprBuilder<CURR>.() -> Expr<T>, sqlType: SqlType<OUT>): NonNullAggrExpr<CURR, OUT> {
        query.checkModifiable()
        val builder = AggrExprBuilderImpl(tableInQuery)
        val expr = builder.block()
        val columnIndex = query.selects.size
        val aggrExpr = NonNullAggrExprImpl<CURR, OUT>(op, expr, columnIndex, sqlType)
        query.selects.add(aggrExpr)
        return aggrExpr
    }

    override fun <T : Any> sum(block: AggrExprBuilder<CURR>.() -> Expr<T>): NullableAggrExpr<CURR, T> {
        return addNullableAggregate(AggrOp.SUM, block)
    }

    override fun <T : Any> min(block: AggrExprBuilder<CURR>.() -> Expr<T>): NullableAggrExpr<CURR, T> {
        return addNullableAggregate(AggrOp.MIN, block)
    }

    override fun <T : Any> max(block: AggrExprBuilder<CURR>.() -> Expr<T>): NullableAggrExpr<CURR, T> {
        return addNullableAggregate(AggrOp.MAX, block)
    }

    override fun <T : Any> count(block: AggrExprBuilder<CURR>.() -> Expr<T>): NonNullAggrExpr<CURR, Long> {
        return addNonNullAggregate(AggrOp.COUNT, block, SqlTypeLong.INSTANCE_FOR_COUNT)
    }

    override fun <T : Any> countDistinct(block: AggrExprBuilder<CURR>.() -> Expr<T>): NonNullAggrExpr<CURR, Long> {
        return addNonNullAggregate(AggrOp.COUNT_DISTINCT, block, SqlTypeLong.INSTANCE_FOR_COUNT)
    }

    override fun <T : Any> average(block: AggrExprBuilder<CURR>.() -> Expr<T>): NullableAggrExpr<CURR, Double> {
        return addNullableAggregate(AggrOp.AVG, block, SqlTypeDouble.INSTANCE_FOR_AVG)
    }

    override fun <T : Any> expr(block: AggrExprBuilder<CURR>.() -> Expr<T>): NonNullAggrExpr<CURR, T> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <T : Any> NonNullColumn<OUT, T>.becomes(literal: T) {
        query.checkModifiable()
        query.outColumns += this
        query.selects.add(makeLiteral(literal))
    }

    override fun <T : Any> NullableColumn<OUT, T>.becomes(literal: T?) {
        query.checkModifiable()
        query.outColumns += this
        if (literal != null) {
            query.selects.add(makeLiteral(literal))
        } else {
            query.selects.add(ExprNull(sqlType))
        }
    }

    override fun <T : Any> NonNullColumn<OUT, T>.becomes(column: NonNullColumn<CURR, T>) {
        query.checkModifiable()
        val boundColumn = column.bindForSelect(tableInQuery)
        query.outColumns += this
        query.selects.add(boundColumn)
        query.groupBy.add(boundColumn)
    }

    override fun <T : Any> NullableColumn<OUT, T>.becomes(column: Column<CURR, T>) {
        query.checkModifiable()
        val boundColumn = column.bindForSelect(tableInQuery)
        query.outColumns += this
        query.selects.add(boundColumn)
        query.groupBy.add(boundColumn)
    }

    override fun <T : Any> Column<OUT, T>.becomes(expr: AggrExpr<CURR, T>) {
        query.outColumns += this
    }

    override fun <REF : DbEntity<REF, *>> innerJoin(ref: RelToSingle<CURR, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = tableInQuery.innerJoin(ref)
        val subBuilder = AggrInsertSelectBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> innerJoin(set: RelToMany<CURR, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = tableInQuery.innerJoin(set)
        val subBuilder = AggrInsertSelectBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> leftJoin(ref: RelToSingle<CURR, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = tableInQuery.leftJoin(ref)
        val subBuilder = AggrInsertSelectBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> leftJoin(set: RelToMany<CURR, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = tableInQuery.leftJoin(set)
        val subBuilder = AggrInsertSelectBuilderImpl(query, subTable)
        subBuilder.block()
    }
}