package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.NonNullColumn
import com.github.mslenc.dbktx.schema.NullableColumn

internal class DbInsertImpl<E : DbEntity<E, ID>, ID : Any>(db: DbConn, table: BaseTableInUpdateQuery<E>)
    : DbMutationImpl<E, ID>(db, table), DbInsert<E, ID> {

    override suspend fun execute(): ID {
        return db.executeInsert(table, values)
    }

    override fun copyUnsetValuesFrom(original: E) {
        for (column in original.metainfo.columns) {
            if (values.getExpr(column) == null) {
                when (column) {
                    is NonNullColumn -> copyNonNull(column, original)
                    is NullableColumn -> copyNullable(column, original)
                }
            }
        }
    }

    private fun <T : Any> copyNonNull(column: NonNullColumn<E, T>, original: E) {
        set(column, column(original))
    }

    private fun <T : Any> copyNullable(column: NullableColumn<E, T>, original: E) {
        set(column, column(original))
    }
}