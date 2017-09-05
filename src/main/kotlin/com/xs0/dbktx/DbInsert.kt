package com.xs0.dbktx

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import si.datastat.db.api.DbEntity
import si.datastat.db.api.DbMutation

interface DbInsert<E : DbEntity<E, ID>, ID> : DbMutation<E> {
    fun execute(handler: (AsyncResult<ID>) -> Unit)
    fun executeE(handler: (ID) -> Unit)

    fun execute(): Future<ID> {
        val future = Future.future<ID>()
        execute({ result ->
            if (result.succeeded()) {
                future.complete(result.result())
            } else {
                future.fail(result.cause())
            }
        })
        return future
    }
}