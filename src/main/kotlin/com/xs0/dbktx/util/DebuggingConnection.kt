package com.xs0.dbktx.util

import com.xs0.asyncdb.common.ResultSet
import com.xs0.asyncdb.vertx.DbConnection
import com.xs0.asyncdb.vertx.TransactionIsolation
import com.xs0.asyncdb.vertx.UpdateResult
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import mu.KLogging

class DebuggingConnection(private val conn: DbConnection) : DbConnection {
    fun <T> wrapForTiming(handler: Handler<AsyncResult<T>>): Handler<AsyncResult<T>> {
        val started = System.currentTimeMillis()
        return Handler { result: AsyncResult<T> ->
            val ended = System.currentTimeMillis()
            logger.debug { "(${ended - started} ms)\n\n" }
            handler.handle(result)
        }
    }

    override fun setAutoCommit(autoCommit: Boolean, resultHandler: Handler<AsyncResult<Void>>): DbConnection {
        return conn.setAutoCommit(autoCommit, resultHandler)
    }

    override fun execute(sql: String, resultHandler: Handler<AsyncResult<Void>>): DbConnection {
        return conn.execute(sql, resultHandler)
    }

    override fun query(sql: String, resultHandler: Handler<AsyncResult<ResultSet>>): DbConnection {
        logger.debug { "Querying with no params:\n$sql" }
        return conn.query(sql, wrapForTiming(resultHandler))
    }

    override fun queryWithParams(sql: String, params: List<Any>, resultHandler: Handler<AsyncResult<ResultSet>>): DbConnection {
        logger.debug { "Querying with params $params:\n$sql" }
        return conn.queryWithParams(sql, params, wrapForTiming(resultHandler))
    }

    override fun update(sql: String, resultHandler: Handler<AsyncResult<UpdateResult>>): DbConnection {
        return conn.update(sql, resultHandler)
    }

    override fun updateWithParams(sql: String, params: List<Any>, resultHandler: Handler<AsyncResult<UpdateResult>>): DbConnection {
        return conn.updateWithParams(sql, params, resultHandler)
    }

    override fun close(handler: Handler<AsyncResult<Void>>) {
        conn.close(handler)
    }

    override fun close() {
        conn.close()
    }

    override fun commit(handler: Handler<AsyncResult<Void>>): DbConnection {
        return conn.commit(handler)
    }

    override fun rollback(handler: Handler<AsyncResult<Void>>): DbConnection {
        return conn.rollback(handler)
    }

    override fun setTransactionIsolation(isolation: TransactionIsolation, handler: Handler<AsyncResult<Void>>): DbConnection {
        return conn.setTransactionIsolation(isolation, handler)
    }

    companion object : KLogging()
}
