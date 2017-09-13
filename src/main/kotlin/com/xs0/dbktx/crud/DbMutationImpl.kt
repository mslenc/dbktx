package com.xs0.dbktx.crud

import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.schema.*

abstract class DbMutationImpl<E : DbEntity<E, ID>, ID: Any> protected constructor(
        protected val db: DbConn,
        protected val table: DbTable<E, ID>) : DbMutation<E> {

    protected val values = EntityValues<E>()

    override operator fun <T: Any>
    set(column: Column<E, T>, value: Expr<in E, T>): DbMutationImpl<E, ID> {
        values.apply {
            column to value
        }
        return this
    }

    override fun <TARGET : DbEntity<TARGET, TID>, TID: Any>
    set(relation: RelToOne<E, TARGET>, target: TARGET): DbMutation<E> {
        @Suppress("UNCHECKED_CAST")
        relation as RelToOneImpl<E, ID, TARGET, TID>

        for (colMap in relation.info.columnMappings) {
            doColMap(colMap, target)
        }

        return this
    }

    private fun <TARGET : DbEntity<TARGET, TID>, TID, VALTYPE: Any>
    doColMap(colMap: ColumnMapping<E, TARGET, VALTYPE>, target: TARGET) {
        val colFrom = colMap.columnFrom
        val colTo = colMap.columnTo

        if (colFrom is NonNullColumn) {
            set(colFrom, colTo.invoke(target))
        } else
        if (colFrom is NullableColumn) {
            set(colFrom, colTo.invoke(target))
        } else {
            throw IllegalStateException("Column is neither nullable or nonNull")
        }
    }
}