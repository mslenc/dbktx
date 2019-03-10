package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.dsl.ColumnUpdateOps
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.ExprBinary
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.filters.FilterBoolean
import com.github.mslenc.dbktx.schema.*

internal class DbUpdateImpl<E : DbEntity<E, ID>, ID: Any>(
        db: DbConn,
        table: DbTable<E, ID>,
        private val specificIds: Set<ID>?,
        private val specificEntity: E?)
    : DbMutationImpl<E, ID>(db, BaseTableInUpdateQuery(UpdateQueryImpl(), table)), DbUpdate<E> {

    private var filters: FilterExpr? = null

    internal fun filter(block: FilterBuilder<E>.()->FilterExpr) {
        val filterBuilder = TableInQueryBoundFilterBuilder(table)
        val filter = filterBuilder.block()

        if (filters == null) {
            filters = filter
        } else {
            filters = FilterBoolean.create(filters!!, FilterBoolean.Op.AND, filter)
        }
    }

    override fun <T : Any> set(column: NonNullColumn<E, T>, value: T) {
        if (specificEntity == null || column(specificEntity) != value)
            values.set(column, value)
    }

    override fun <T : Any> set(column: NullableColumn<E, T>, value: T?) {
        if (specificEntity == null || column(specificEntity) != value)
            values.set(column, value)
    }

    override fun <T : Any> set(column: Column<E, T>, value: Expr<E, T>) {
        values.set(column, value)
    }

    override fun <T : Any> get(column: Column<E, T>): DbColumnMutation<E, T> {
        return DbColumnMutationImpl(this, column)
    }

    override suspend fun execute(): Long {
        return db.executeUpdate(table, filters, values, specificIds)
    }
}

internal class DbColumnMutationImpl<E: DbEntity<E, *>, T: Any>(private val update: DbUpdateImpl<E, *>, private val column: Column<E, T>) : DbColumnMutation<E, T> {
    fun setDeltaValue(op: ExprBinary.Op, delta: Expr<E, T>) {
        update[column] = ExprBinary(column.bindForSelect(update.table), op, delta)
    }

    override fun plusAssign(value: T) {
        plusAssign(column.makeLiteral(value))
    }

    override fun minusAssign(value: T) {
        minusAssign(column.makeLiteral(value))
    }

    override fun timesAssign(value: T) {
        timesAssign(column.makeLiteral(value))
    }

    override fun divAssign(value: T) {
        divAssign(column.makeLiteral(value))
    }

    override fun remAssign(value: T) {
        remAssign(column.makeLiteral(value))
    }

    override fun plusAssign(value: Expr<E, T>) {
        setDeltaValue(ExprBinary.Op.PLUS, value)
    }

    override fun minusAssign(value: Expr<E, T>) {
        setDeltaValue(ExprBinary.Op.MINUS, value)
    }

    override fun timesAssign(value: Expr<E, T>) {
        setDeltaValue(ExprBinary.Op.TIMES, value)
    }

    override fun divAssign(value: Expr<E, T>) {
        setDeltaValue(ExprBinary.Op.DIV, value)
    }

    override fun remAssign(value: Expr<E, T>) {
        setDeltaValue(ExprBinary.Op.REM, value)
    }

    override fun becomes(value: ColumnUpdateOps<E, T>.() -> Expr<E, T>) {
        update[column] = ColumnUpdateOpsImpl(column, update).value()
    }
}

internal class ColumnUpdateOpsImpl<E : DbEntity<E, *>, T : Any>(internal val column: Column<E, T>, internal val update: DbUpdateImpl<E, *>) : ColumnUpdateOps<E, T> {
    override fun literal(value: T): Expr<E, T> {
        return column.makeLiteral(value)
    }

    override fun bind(column: Column<E, T>): Expr<E, T> {
        return column.bindForSelect(update.table)
    }
}