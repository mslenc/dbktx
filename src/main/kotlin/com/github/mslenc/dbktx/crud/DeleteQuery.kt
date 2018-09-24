package com.github.mslenc.dbktx.crud

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.schema.*

interface DeleteQuery<E : DbEntity<E, *>>: FilterableQuery<E> {
    suspend fun deleteAllMatchingRows(): Long
}

internal class DeleteQueryImpl<E : DbEntity<E, *>>(
        table: DbTable<E, *>,
        loader: DbConn)
    : FilterableQueryImpl<E>(table, loader), DeleteQuery<E> {

    override fun makeBaseTable(table: DbTable<E, *>): TableInQuery<E> {
        return BaseTableInUpdateQuery(this, table)
    }

    var executed = false

    override suspend fun deleteAllMatchingRows(): Long {
        if (filters == null)
            throw IllegalStateException("No filter specified, don't want to delete entire table")

        checkModifiable()

        executed = true
        return loader.executeDelete(this)
    }

    override fun checkModifiable() {
        if (executed)
            throw IllegalStateException("Already deleting")
    }
}
