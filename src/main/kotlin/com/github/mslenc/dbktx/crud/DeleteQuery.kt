package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.filters.MatchAnything
import com.github.mslenc.dbktx.schema.*

interface DeleteQuery<E : DbEntity<E, *>>: FilterableQuery<E> {
    suspend fun deleteAllMatchingRows(): Long
}

internal class DeleteQueryImpl<E : DbEntity<E, *>>(
        table: DbTable<E, *>,
        db: DbConn)
    : FilterableQueryImpl<E>(table, db), DeleteQuery<E> {

    override val aggregatesAllowed: Boolean
        get() = false

    override fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E> {
        return BaseTableInUpdateQuery(this, table)
    }

    var executed = false

    override suspend fun deleteAllMatchingRows(): Long {
        if (filters == MatchAnything)
            throw IllegalStateException("No filter specified, don't want to delete entire table")

        checkModifiable()

        executed = true
        return db.executeDelete(this)
    }

    override fun checkModifiable() {
        if (executed)
            throw IllegalStateException("Already deleting")
    }
}
