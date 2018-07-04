package com.xs0.dbktx.util

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.ext.sql.*
import mu.KLogging

class DebuggingConnection(private val conn: SQLConnection) : SQLConnection {
    fun <T> wrapForTiming(handler: Handler<AsyncResult<T>>): Handler<AsyncResult<T>> {
        val started = System.currentTimeMillis()
        return Handler { result: AsyncResult<T> ->
            val ended = System.currentTimeMillis()
            logger.debug { "(${ended - started} ms)\n\n" }
            handler.handle(result)
        }
    }

    override fun setOptions(options: SQLOptions): SQLConnection {
        return conn.setOptions(options)
    }

    override fun setAutoCommit(autoCommit: Boolean, resultHandler: Handler<AsyncResult<Void>>): SQLConnection {
        return conn.setAutoCommit(autoCommit, resultHandler)
    }

    override fun execute(sql: String, resultHandler: Handler<AsyncResult<Void>>): SQLConnection {
        return conn.execute(sql, resultHandler)
    }

    override fun query(sql: String, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        logger.debug { "Querying with no params:\n$sql" }
        return conn.query(sql, wrapForTiming(resultHandler))
    }

    override fun queryStream(sql: String, handler: Handler<AsyncResult<SQLRowStream>>): SQLConnection {
        return conn.queryStream(sql, handler)
    }

    override fun queryWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        logger.debug { "Querying with params $params:\n$sql" }
        return conn.queryWithParams(sql, params, wrapForTiming(resultHandler))
    }

    override fun queryStreamWithParams(sql: String, params: JsonArray, handler: Handler<AsyncResult<SQLRowStream>>): SQLConnection {
        return conn.queryStreamWithParams(sql, params, handler)
    }

    override fun update(sql: String, resultHandler: Handler<AsyncResult<UpdateResult>>): SQLConnection {
        return conn.update(sql, resultHandler)
    }

    override fun updateWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<UpdateResult>>): SQLConnection {
        return conn.updateWithParams(sql, params, resultHandler)
    }

    override fun call(sql: String, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        return conn.call(sql, resultHandler)
    }

    override fun callWithParams(sql: String, params: JsonArray, outputs: JsonArray, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        return conn.callWithParams(sql, params, outputs, resultHandler)
    }

    override fun close(handler: Handler<AsyncResult<Void>>) {
        conn.close(handler)
    }

    override fun close() {
        conn.close()
    }

    override fun commit(handler: Handler<AsyncResult<Void>>): SQLConnection {
        return conn.commit(handler)
    }

    override fun rollback(handler: Handler<AsyncResult<Void>>): SQLConnection {
        return conn.rollback(handler)
    }

    @Deprecated("")
    override fun setQueryTimeout(timeoutInSeconds: Int): SQLConnection {
        return conn.setQueryTimeout(timeoutInSeconds)
    }

    override fun batch(sqlStatements: List<String>, handler: Handler<AsyncResult<List<Int>>>): SQLConnection {
        return conn.batch(sqlStatements, handler)
    }

    override fun batchWithParams(sqlStatement: String, args: List<JsonArray>, handler: Handler<AsyncResult<List<Int>>>): SQLConnection {
        return conn.batchWithParams(sqlStatement, args, handler)
    }

    override fun batchCallableWithParams(sqlStatement: String, inArgs: List<JsonArray>, outArgs: List<JsonArray>, handler: Handler<AsyncResult<List<Int>>>): SQLConnection {
        return conn.batchCallableWithParams(sqlStatement, inArgs, outArgs, handler)
    }

    override fun setTransactionIsolation(isolation: TransactionIsolation, handler: Handler<AsyncResult<Void>>): SQLConnection {
        return conn.setTransactionIsolation(isolation, handler)
    }

    override fun getTransactionIsolation(handler: Handler<AsyncResult<TransactionIsolation>>): SQLConnection {
        return conn.getTransactionIsolation(handler)
    }

    override fun <N> unwrap(): N {
        return conn.unwrap()
    }

    override fun querySingle(sql: String, handler: Handler<AsyncResult<JsonArray>>): SQLOperations {
        return conn.querySingle(sql, handler)
    }

    override fun querySingleWithParams(sql: String, arguments: JsonArray, handler: Handler<AsyncResult<JsonArray>>): SQLOperations {
        return conn.querySingleWithParams(sql, arguments, handler)
    }

    companion object : KLogging()
}
