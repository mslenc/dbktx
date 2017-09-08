package com.xs0.dbktx.crud

import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable

class DbInsertImpl<E : DbEntity<E, ID>, ID: Any>(db: DbConn, table: DbTable<E, ID>)
    : DbMutationImpl<E, ID>(db, table), DbInsert<E, ID> {

    override suspend fun execute(): ID {
        return db.executeInsert(table, values)
    }
}