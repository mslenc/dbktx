package com.xs0.dbktx

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import si.datastat.db.api.DbEntity
import si.datastat.db.api.DbMutation

interface DbUpdate<E : DbEntity<E, *>> : DbMutation<E> {
    fun execute(handler: (AsyncResult<Long>)->Unit)
    fun executeE(onSuccess: (Long)->Unit)

    fun execute(): Future<Long> {
        val future = Future.future<Long>()
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