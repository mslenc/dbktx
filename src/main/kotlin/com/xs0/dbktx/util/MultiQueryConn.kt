package com.xs0.dbktx.util

import com.github.mauricio.async.db.exceptions.DatabaseException
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.ext.sql.*
import mu.KLogging
import java.io.PrintWriter
import java.io.StringWriter

class MultiQueryConn(private val conn: SQLConnection) : SQLConnection {
    private var executing: String? = null
    private var closed: Boolean = false

    private fun <T> wrap(handler: Handler<AsyncResult<T>>): Handler<AsyncResult<T>> {
        return Handler { result ->
            executing = null
            handler.handle(result)
        }
    }

    private fun <T> checkState(handler: Handler<AsyncResult<T>>): Boolean {
        if (closed) {
            handler.handle(Future.failedFuture(DatabaseException("Connection is already closed")))
            return false
        }

        if (executing != null) {
            handler.handle(Future.failedFuture(DatabaseException("Already executing a query:\n$executing")))
            return false
        }

        try {
            throw Throwable()
        } catch (t: Throwable) {
            val s = StringWriter()
            val p = PrintWriter(s)
            t.printStackTrace(p)
            p.flush()
            executing = s.toString()
        }

        return true
    }

    override fun setAutoCommit(autoCommit: Boolean, resultHandler: Handler<AsyncResult<Void>>): SQLConnection {
        if (checkState(resultHandler))
            conn.setAutoCommit(autoCommit, wrap(resultHandler))

        return this
    }

    override fun execute(sql: String, resultHandler: Handler<AsyncResult<Void>>): SQLConnection {
        if (checkState(resultHandler))
            conn.execute(sql, wrap(resultHandler))

        return this
    }

    override fun query(sql: String, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        if (checkState(resultHandler))
            conn.query(sql, wrap(resultHandler))

        return this
    }

    override fun queryStream(sql: String, resultHandler: Handler<AsyncResult<SQLRowStream>>): SQLConnection {
        if (checkState(resultHandler))
            conn.queryStream(sql, wrap(resultHandler))

        return this
    }

    override fun queryWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        if (params.isEmpty)
            return query(sql, resultHandler)

        if (checkState(resultHandler))
            conn.queryWithParams(sql, params, wrap(resultHandler))

        return this
    }

    override fun queryStreamWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<SQLRowStream>>): SQLConnection {
        if (params.isEmpty)
            return queryStream(sql, resultHandler)

        if (checkState(resultHandler))
            conn.queryStreamWithParams(sql, params, wrap(resultHandler))

        return this
    }

    override fun update(sql: String, resultHandler: Handler<AsyncResult<UpdateResult>>): SQLConnection {
        if (checkState(resultHandler))
            conn.update(sql, wrap(resultHandler))

        return this
    }

    override fun updateWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<UpdateResult>>): SQLConnection {
        if (params.isEmpty)
            return update(sql, resultHandler)

        if (checkState(resultHandler))
            conn.updateWithParams(sql, params, wrap(resultHandler))

        return this
    }

    override fun call(sql: String, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        if (checkState(resultHandler))
            conn.call(sql, wrap(resultHandler))

        return this
    }

    override fun callWithParams(sql: String, params: JsonArray, outputs: JsonArray, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        if (params.isEmpty)
            return call(sql, resultHandler)

        if (checkState(resultHandler))
            conn.callWithParams(sql, params, outputs, wrap(resultHandler))

        return this
    }

    override fun close(resultHandler: Handler<AsyncResult<Void>>) {
        if (checkState(resultHandler)) {
            closed = true
            logger.debug("Closing connection")
            conn.close(wrap(resultHandler))
        } else {
            closed = true
            logger.error("close() called while executing.. Unless the query aborted just before this, it is a bug")
            // but we still close the connection, otherwise it will probably linger forever
            conn.close { _ -> }
        }
    }

    override fun close() {
        close { /* ignore.. */ _ -> }
    }

    override fun commit(resultHandler: Handler<AsyncResult<Void>>): SQLConnection {
        if (checkState(resultHandler))
            conn.commit(wrap(resultHandler))

        return this
    }

    override fun rollback(resultHandler: Handler<AsyncResult<Void>>): SQLConnection {
        if (checkState(resultHandler))
            conn.rollback(wrap(resultHandler))

        return this
    }

    override fun setQueryTimeout(timeoutInSeconds: Int): SQLConnection {
        conn.setQueryTimeout(timeoutInSeconds)
        return this
    }

    override fun batch(sqlStatements: List<String>, resultHandler: Handler<AsyncResult<List<Int>>>): SQLConnection {
        if (checkState(resultHandler))
            conn.batch(sqlStatements, wrap(resultHandler))

        return this
    }

    override fun batchWithParams(sqlStatement: String, args: List<JsonArray>, resultHandler: Handler<AsyncResult<List<Int>>>): SQLConnection {
        if (checkState(resultHandler))
            conn.batchWithParams(sqlStatement, args, wrap(resultHandler))

        return this
    }

    override fun batchCallableWithParams(sqlStatement: String, inArgs: List<JsonArray>, outArgs: List<JsonArray>, resultHandler: Handler<AsyncResult<List<Int>>>): SQLConnection {
        if (checkState(resultHandler))
            conn.batchCallableWithParams(sqlStatement, inArgs, outArgs, wrap(resultHandler))

        return this
    }

    override fun setTransactionIsolation(isolation: TransactionIsolation, resultHandler: Handler<AsyncResult<Void>>): SQLConnection {
        if (checkState(resultHandler))
            conn.setTransactionIsolation(isolation, wrap(resultHandler))

        return this
    }

    override fun getTransactionIsolation(resultHandler: Handler<AsyncResult<TransactionIsolation>>): SQLConnection {
        if (checkState(resultHandler))
            conn.getTransactionIsolation(wrap(resultHandler))

        return this
    }

    override fun setOptions(options: SQLOptions?): SQLConnection {
        conn.setOptions(options)
        return this
    }

    companion object : KLogging()
}
