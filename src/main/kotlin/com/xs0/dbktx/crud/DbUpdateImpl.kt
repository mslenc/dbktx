package com.xs0.dbktx.crud

import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.expr.ExprBools
import com.xs0.dbktx.schema.*

internal class DbUpdateImpl<E : DbEntity<E, ID>, ID: Any>(
        db: DbConn,
        table: DbTable<E, ID>,
        private val specificIds: Set<ID>?,
        private val specificEntity: E?)
    : DbMutationImpl<E, ID>(db, BaseTableInUpdateQuery(UpdateQueryImpl(), table)), DbUpdate<E> {

    private var filters: ExprBoolean? = null

    internal fun filter(block: FilterBuilder<E>.()->ExprBoolean) {
        val filterBuilder = TableInQueryBoundFilterBuilder(table)
        val filter = filterBuilder.block()

        if (filters == null) {
            filters = filter
        } else {
            filters = ExprBools.create(filters!!, ExprBools.Op.AND, filter)
        }
    }

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
        return db.executeUpdate(table, filters, values, specificIds)
    }
}
