package com.github.mslenc.dbktx.schema

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.crud.DbUpdate

abstract class DbEntity<E : DbEntity<E, ID>, ID: Any>(
    val db: DbConn,
    val id: ID,
    val row: DbRow
) {

    abstract val metainfo: DbTable<E, ID>

    fun createUpdate(): DbUpdate<E> {
        return metainfo.updateById(db, id)
    }

    suspend fun executeUpdate(modifier: DbUpdate<E>.() -> Unit): Boolean {
        val update = createUpdate()
        update.modifier()
        return update.execute() > 0
    }
}
