package com.xs0.dbktx.schema

import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.crud.DbUpdate

abstract class DbEntity<E : DbEntity<E, ID>, ID: Any>(
    val db: DbConn,
    val id: ID

) {

    abstract val metainfo: DbTable<E, ID>

    fun update(db: DbConn): DbUpdate<E> {
        return metainfo.updateById(db, id)
    }
}
