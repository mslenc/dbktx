package com.xs0.dbktx.crud

import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.schema.*

internal class DbUpdateImpl<E : DbEntity<E, ID>, ID: Any>(
        db: DbConn,
        table: BaseTableInUpdateQuery<E>,
        private val filter: ExprBoolean?,
        private val specificIds: Set<ID>?,
        private val specificEntity: E?)
    : DbMutationImpl<E, ID>(db, table), DbUpdate<E> {

    override fun <T : Any> set(column: NonNullColumn<E, T>, value: T): DbUpdate<E> {
        if (specificEntity == null || column(specificEntity) != value)
            values.set(column, value)

        return this
    }

    override fun <T : Any> set(column: NullableColumn<E, T>, value: T?): DbUpdate<E> {
        if (specificEntity == null || column(specificEntity) != value)
            values.set(column, value)

        return this
    }

    override fun <T : Any> set(column: Column<E, T>, value: Expr<E, T>): DbUpdate<E> {
        values.set(column, value)
        return this
    }

    override fun <T : Any> setNull(column: NullableColumn<E, T>): DbUpdate<E> {
        if (specificEntity == null || column(specificEntity) != null)
            values.set(column, (null as T?))

        return this
    }

    override suspend fun execute(): Long {
        return db.executeUpdate(table, filter, values, specificIds)
    }
}
