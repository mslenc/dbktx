package com.github.mslenc.dbktx.aggr

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.*
import com.github.mslenc.dbktx.crud.OrderableFilterableQueryImpl
import com.github.mslenc.dbktx.expr.*
import com.github.mslenc.dbktx.schema.*
import com.github.mslenc.dbktx.util.Sql

internal class AggrInsertSelectQueryImpl<OUT: DbEntity<OUT, *>, ROOT: DbEntity<ROOT, *>>(val outTable: DbTable<OUT, *>, queryRoot: DbTable<ROOT, *>, db: DbConn) : OrderableFilterableQueryImpl<ROOT>(queryRoot, db), AggrInsertSelectQuery<OUT, ROOT> {
    override val aggregatesAllowed: Boolean
        get() = true

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

    override fun expand(block: AggrInsertSelectTopLevelBuilder<OUT, ROOT>.() -> Unit) {
        checkModifiable()
        val builder = AggrInsertSelectBuilderImpl(this, table)
        builder.block()
    }
}

internal class AggrInsertSelectBuilderImpl<OUT: DbEntity<OUT, *>, ROOT: DbEntity<ROOT, *>, CURR: DbEntity<CURR, *>>(val query: AggrInsertSelectQueryImpl<OUT, ROOT>, override val table: TableInQuery<CURR>): AggrInsertSelectTopLevelBuilder<OUT, CURR>, FilterableQuery<CURR> {
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

    override fun <T : Any> expr(block: AggrExprBuilder<CURR>.() -> Expr<T>): Expr<T> {
        return table.newExprBuilder().block()
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
        val boundColumn = column.bindForSelect(this@AggrInsertSelectBuilderImpl.table)
        query.outColumns += this
        query.selects.add(boundColumn)
        query.groupBy.add(boundColumn)
    }

    override fun <T : Any> NullableColumn<OUT, T>.becomes(column: Column<CURR, T>) {
        query.checkModifiable()
        val boundColumn = column.bindForSelect(this@AggrInsertSelectBuilderImpl.table)
        query.outColumns += this
        query.selects.add(boundColumn)
        query.groupBy.add(boundColumn)
    }

    override fun <T : Any> Column<OUT, T>.becomes(expr: Expr<T>) {
        query.outColumns += this
        query.selects += expr
    }

    override fun <REF : DbEntity<REF, *>> innerJoin(ref: RelToSingle<CURR, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = table.innerJoin(ref)
        val subBuilder = AggrInsertSelectBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> innerJoin(set: RelToMany<CURR, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = table.innerJoin(set)
        val subBuilder = AggrInsertSelectBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> leftJoin(ref: RelToSingle<CURR, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = table.leftJoin(ref)
        val subBuilder = AggrInsertSelectBuilderImpl(query, subTable)
        subBuilder.block()
    }

    override fun <REF : DbEntity<REF, *>> leftJoin(set: RelToMany<CURR, REF>, block: AggrInsertSelectBuilder<OUT, REF>.() -> Unit) {
        query.checkModifiable()
        val subTable = table.leftJoin(set)
        val subBuilder = AggrInsertSelectBuilderImpl(query, subTable)
        subBuilder.block()
    }
}