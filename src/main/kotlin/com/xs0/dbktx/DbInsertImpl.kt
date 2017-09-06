package com.xs0.dbktx

import io.vertx.core.AsyncResult
import io.vertx.core.Handler

class DbInsertImpl<E : DbEntity<E, ID>, ID: Any>(db: DbConn<*>, table: DbTable<E, ID>)
    : DbMutationImpl<E, ID>(db, table), DbInsert<E, ID> {

    override suspend fun execute(): ID {
        return db.executeInsert(table, values)
    }

    fun executeE(handler: Handler<ID>) {
        db.executeInsertE(table, values, handler)
    }
}