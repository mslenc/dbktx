package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.schema.*

internal abstract class DbMutationImpl<E : DbEntity<E, ID>, ID: Any> protected constructor(
        protected val db: DbConn,
        override val table: BaseTableInUpdateQuery<E>) : DbMutation<E> {

    protected val values = EntityValues<E>()

    override fun <T : Any> set(column: NonNullColumn<E, T>, value: T): DbMutation<E> {
        values.set(column, value)
        return this
    }

    override fun <T : Any> set(column: NullableColumn<E, T>, value: T?): DbMutation<E> {
        values.set(column, value)
        return this
    }

    override fun <T : Any> set(column: Column<E, T>, value: Expr<E, T>): DbMutation<E> {
        values.set(column, value)
        return this
    }

    override fun <T : Any> setNull(column: NullableColumn<E, T>): DbMutation<E> {
        values.set(column, (null as T?))
        return this
    }

    override fun <TARGET : DbEntity<TARGET, TID>, TID: Any>
    set(relation: RelToOne<E, TARGET>, target: TARGET): DbMutation<E> {
        @Suppress("UNCHECKED_CAST")
        relation as RelToOneImpl<E, TARGET, TID>

        for (colMap in relation.info.columnMappings) {
            doColMap(colMap, target)
        }

        return this
    }

    private fun <TARGET : DbEntity<TARGET, TID>, TID, VALTYPE: Any>
    doColMap(colMap: ColumnMapping<E, TARGET, VALTYPE>, target: TARGET) {
        if (colMap.columnFromKind != ColumnInMappingKind.COLUMN)
            return // TODO: check that constants and parameters match target?

        val colFrom = colMap.rawColumnFrom
        val colTo = colMap.rawColumnTo

        when (colFrom) {
            is NonNullColumn -> set(colFrom, colTo.invoke(target))
            is NullableColumn -> set(colFrom, colTo.invoke(target))
            else -> throw IllegalStateException("Column is neither nullable or nonNull")
        }
    }
}