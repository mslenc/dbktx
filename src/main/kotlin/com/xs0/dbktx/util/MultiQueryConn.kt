package com.xs0.dbktx.util

import com.github.mauricio.async.db.exceptions.DatabaseException
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.ext.sql.*
import mu.KLogging

import java.util.LinkedList

class MultiQueryConn(private val conn: SQLConnection) : SQLConnection {
    private val pending = LinkedList<()->Unit>()
    private var executing: Boolean = false
    private var closed: Boolean = false

    private fun <T> wrap(handler: Handler<AsyncResult<T>>): Handler<AsyncResult<T>> {
        return Handler { result ->
            if (pending.isEmpty()) {
                executing = false
            } else {
                pending.removeFirst()()
            }

            handler.handle(result)
        }
    }

    private fun <T> checkClosed(handler: Handler<AsyncResult<T>>): Boolean {
        if (closed) {
            handler.handle(Future.failedFuture(DatabaseException("Connection is already closed")))
            return true
        } else {
            return false
        }
    }

    override fun setAutoCommit(autoCommit: Boolean, resultHandler: Handler<AsyncResult<Void>>): SQLConnection {
        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.setAutoCommit(autoCommit, handler) })
        } else {
            executing = true
            conn.setAutoCommit(autoCommit, handler)
        }

        return this
    }

    override fun execute(sql: String, resultHandler: Handler<AsyncResult<Void>>): SQLConnection {
        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.execute(sql, handler) })
        } else {
            executing = true
            conn.execute(sql, handler)
        }

        return this
    }

    override fun query(sql: String, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.query(sql, handler) })
        } else {
            executing = true
            conn.query(sql, handler)
        }

        return this
    }

    override fun queryStream(sql: String, resultHandler: Handler<AsyncResult<SQLRowStream>>): SQLConnection {
        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.queryStream(sql, handler) })
        } else {
            executing = true
            conn.queryStream(sql, handler)
        }

        return this
    }

    override fun queryWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        if (params.isEmpty)
            return query(sql, resultHandler)

        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.queryWithParams(sql, params, handler) })
        } else {
            executing = true
            conn.queryWithParams(sql, params, handler)
        }

        return this
    }

    override fun queryStreamWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<SQLRowStream>>): SQLConnection {
        if (params.isEmpty)
            return queryStream(sql, resultHandler)

        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.queryStreamWithParams(sql, params, handler) })
        } else {
            executing = true
            conn.queryStreamWithParams(sql, params, handler)
        }

        return this
    }

    override fun update(sql: String, resultHandler: Handler<AsyncResult<UpdateResult>>): SQLConnection {
        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.update(sql, handler) })
        } else {
            executing = true
            conn.update(sql, handler)
        }

        return this
    }

    override fun updateWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<UpdateResult>>): SQLConnection {
        if (params.isEmpty)
            return update(sql, resultHandler)

        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.updateWithParams(sql, params, handler) })
        } else {
            executing = true
            conn.updateWithParams(sql, params, handler)
        }

        return this
    }

    override fun call(sql: String, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.call(sql, handler) })
        } else {
            executing = true
            conn.call(sql, handler)
        }

        return this
    }

    override fun callWithParams(sql: String, params: JsonArray, outputs: JsonArray, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        if (params.isEmpty)
            return call(sql, resultHandler)

        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.callWithParams(sql, params, outputs, handler) })
        } else {
            executing = true
            conn.callWithParams(sql, params, outputs, handler)
        }

        return this
    }

    override fun close(resultHandler: Handler<AsyncResult<Void>>) {
        if (checkClosed(resultHandler))
            return

        val handler = wrap(resultHandler)
        closed = true // further calls will happen after close, so no point in accepting them..

        if (executing) {
            pending.add({ conn.close(handler) })
        } else {
            executing = true
            conn.close(handler)
        }
    }

    override fun close() {
        close { /* ignore.. */ result -> }
    }

    override fun commit(resultHandler: Handler<AsyncResult<Void>>): SQLConnection {
        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)
        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.commit(handler) })
        } else {
            executing = true
            conn.commit(handler)
        }

        return this
    }

    override fun rollback(resultHandler: Handler<AsyncResult<Void>>): SQLConnection {
        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.rollback(handler) })
        } else {
            executing = true
            conn.rollback(handler)
        }

        return this
    }

    override fun setQueryTimeout(timeoutInSeconds: Int): SQLConnection {
        conn.setQueryTimeout(timeoutInSeconds)
        return this
    }

    override fun batch(sqlStatements: List<String>, resultHandler: Handler<AsyncResult<List<Int>>>): SQLConnection {
        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.batch(sqlStatements, handler) })
        } else {
            executing = true
            conn.batch(sqlStatements, handler)
        }

        return this
    }

    override fun batchWithParams(sqlStatement: String, args: List<JsonArray>, resultHandler: Handler<AsyncResult<List<Int>>>): SQLConnection {
        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.batchWithParams(sqlStatement, args, handler) })
        } else {
            executing = true
            conn.batchWithParams(sqlStatement, args, handler)
        }

        return this
    }

    override fun batchCallableWithParams(sqlStatement: String, inArgs: List<JsonArray>, outArgs: List<JsonArray>, resultHandler: Handler<AsyncResult<List<Int>>>): SQLConnection {
        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.batchCallableWithParams(sqlStatement, inArgs, outArgs, handler) })
        } else {
            executing = true
            conn.batchCallableWithParams(sqlStatement, inArgs, outArgs, handler)
        }

        return this
    }

    override fun setTransactionIsolation(isolation: TransactionIsolation, resultHandler: Handler<AsyncResult<Void>>): SQLConnection {
        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.setTransactionIsolation(isolation, handler) })
        } else {
            executing = true
            conn.setTransactionIsolation(isolation, handler)
        }

        return this
    }

    override fun getTransactionIsolation(resultHandler: Handler<AsyncResult<TransactionIsolation>>): SQLConnection {
        if (checkClosed(resultHandler))
            return this

        val handler = wrap(resultHandler)

        if (executing) {
            pending.add({ if (!checkClosed(handler)) conn.getTransactionIsolation(handler) })
        } else {
            executing = true
            conn.getTransactionIsolation(handler)
        }

        return this
    }

    companion object : KLogging()
}
