package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.dsl.ColumnUpdateOps
import com.github.mslenc.dbktx.expr.*
import com.github.mslenc.dbktx.expr.ExprBinary
import com.github.mslenc.dbktx.filters.MatchAnything
import com.github.mslenc.dbktx.schema.*

internal class DbUpdateImpl<E : DbEntity<E, ID>, ID: Any>(
        db: DbConn,
        table: DbTable<E, ID>,
        private val specificEntity: E?)
    : DbMutationImpl<E, ID>(db, BaseTableInUpdateQuery(UpdateQueryImpl(), table)), DbUpdate<E> {

    private var filters: Expr<Boolean> = MatchAnything
    private val vals: MutableMap<Column<E, *>, Any?> = LinkedHashMap()
    private val exprs: MutableMap<Column<E, *>, Expr<*>> = LinkedHashMap()

    override fun filter(block: ExprBuilder<E>.()->Expr<Boolean>) {
        filters = filters and table.newExprBuilder().block()
    }

    override fun <T : Any> set(column: NonNullColumn<E, T>, value: T) {
        if (specificEntity == null || column(specificEntity) != value) {
            vals[column] = value
            exprs[column] = column.makeLiteral(value)
        } else {
            vals.remove(column)
            exprs.remove(column)
        }
    }

    override fun <T : Any> set(column: NullableColumn<E, T>, value: T?) {
        if (specificEntity == null || column(specificEntity) != value) {
            vals[column] = value
            exprs[column] = if (value != null) column.makeLiteral(value) else ExprNull(column.sqlType)
        } else {
            vals.remove(column)
            exprs.remove(column)
        }
    }

    override fun <T : Any> set(column: Column<E, T>, value: Expr<T>) {
        exprs[column] = value
        vals.remove(column)
    }

    override fun <T : Any> get(column: Column<E, T>): DbColumnMutation<E, T> {
        return DbColumnMutationImpl(this, column)
    }

    override suspend fun execute(): Long {
        return db.executeUpdate(table, filters, exprs)
    }

    override fun anyChangesSoFar(): Boolean {
        return vals.isNotEmpty()
    }
}

internal class DbColumnMutationImpl<E: DbEntity<E, *>, T: Any>(private val update: DbUpdateImpl<E, *>, private val column: Column<E, T>) : DbColumnMutation<E, T> {
    fun setDeltaValue(op: BinaryOp, delta: Expr<T>) {
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

    override fun plusAssign(value: Expr<T>) {
        setDeltaValue(BinaryOp.PLUS, value)
    }

    override fun minusAssign(value: Expr<T>) {
        setDeltaValue(BinaryOp.MINUS, value)
    }

    override fun timesAssign(value: Expr<T>) {
        setDeltaValue(BinaryOp.TIMES, value)
    }

    override fun divAssign(value: Expr<T>) {
        setDeltaValue(BinaryOp.DIV, value)
    }

    override fun remAssign(value: Expr<T>) {
        setDeltaValue(BinaryOp.REM, value)
    }

    override fun becomes(value: ColumnUpdateOps<E, T>.() -> Expr<T>) {
        update[column] = ColumnUpdateOpsImpl(column, update).value()
    }
}

internal class ColumnUpdateOpsImpl<E : DbEntity<E, *>, T : Any>(internal val column: Column<E, T>, internal val update: DbUpdateImpl<E, *>) : ColumnUpdateOps<E, T> {
    override fun literal(value: T): Expr<T> {
        return column.makeLiteral(value)
    }

    override fun bind(column: Column<E, T>): Expr<T> {
        return column.bindForSelect(update.table)
    }
}