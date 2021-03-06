package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.ExprNull
import com.github.mslenc.dbktx.schema.*

internal abstract class DbMutationImpl<E : DbEntity<E, ID>, ID: Any> protected constructor(
        protected val db: DbConn,
        override val table: BaseTableInUpdateQuery<E>) : DbMutation<E> {

    override fun <TARGET : DbEntity<TARGET, TID>, TID: Any>
    set(relation: RelToOne<E, TARGET>, target: TARGET) {
        @Suppress("UNCHECKED_CAST")
        relation as RelToOneImpl<E, TARGET, TID>

        for (colMap in relation.info.columnMappings) {
            doColMap(colMap, target)
        }
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