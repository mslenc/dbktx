package com.xs0.dbktx.crud

import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.expr.ExprBoolean
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable

class DbUpdateImpl<E : DbEntity<E, ID>, ID: Any>(
        db: DbConn,
        table: DbTable<E, ID>,
        private val filter: ExprBoolean<E>?,
        private val specificIds: Set<ID>?)
    : DbMutationImpl<E, ID>(db, table), DbUpdate<E> {

    override suspend fun execute(): Long {
        return db.executeUpdate(table, filter, values, specificIds)
    }
}
