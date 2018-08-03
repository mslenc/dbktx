package com.xs0.dbktx.util.testing

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.ext.sql.*

open class MockSQLConnection : SQLConnection {
    override fun updateWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<UpdateResult>>): SQLConnection {
        TODO("not implemented")
    }

    override fun call(sql: String, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        TODO("not implemented")
    }

    override fun queryStreamWithParams(sql: String, params: JsonArray, handler: Handler<AsyncResult<SQLRowStream>>): SQLConnection {
        TODO("not implemented")
    }

    override fun queryStream(sql: String, handler: Handler<AsyncResult<SQLRowStream>>): SQLConnection {
        TODO("not implemented")
    }

    override fun rollback(handler: Handler<AsyncResult<Void>>): SQLConnection {
        TODO("not implemented")
    }

    override fun callWithParams(sql: String, params: JsonArray, outputs: JsonArray, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        TODO("not implemented")
    }

    override fun batchWithParams(sqlStatement: String, args: MutableList<JsonArray>, handler: Handler<AsyncResult<MutableList<Int>>>): SQLConnection {
        TODO("not implemented")
    }

    override fun query(sql: String, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        TODO("not implemented")
    }

    override fun commit(handler: Handler<AsyncResult<Void>>): SQLConnection {
        TODO("not implemented")
    }

    override fun execute(sql: String, resultHandler: Handler<AsyncResult<Void>>): SQLConnection {
        TODO("not implemented")
    }

    override fun batch(sqlStatements: MutableList<String>, handler: Handler<AsyncResult<MutableList<Int>>>): SQLConnection {
        TODO("not implemented")
    }

    override fun setTransactionIsolation(isolation: TransactionIsolation, handler: Handler<AsyncResult<Void>>): SQLConnection {
        TODO("not implemented")
    }

    override fun setAutoCommit(autoCommit: Boolean, resultHandler: Handler<AsyncResult<Void>>): SQLConnection {
        TODO("not implemented")
    }

    override fun queryWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
        TODO("not implemented")
    }

    override fun update(sql: String, resultHandler: Handler<AsyncResult<UpdateResult>>): SQLConnection {
        TODO("not implemented")
    }

    override fun getTransactionIsolation(handler: Handler<AsyncResult<TransactionIsolation>>): SQLConnection {
        TODO("not implemented")
    }

    override fun close(handler: Handler<AsyncResult<Void>>) {
        TODO("not implemented")
    }

    override fun close() {
        TODO("not implemented")
    }

    override fun setQueryTimeout(timeoutInSeconds: Int): SQLConnection {
        TODO("not implemented")
    }

    override fun batchCallableWithParams(sqlStatement: String, inArgs: MutableList<JsonArray>, outArgs: MutableList<JsonArray>, handler: Handler<AsyncResult<MutableList<Int>>>): SQLConnection {
        TODO("not implemented")
    }

    override fun setOptions(options: SQLOptions?): SQLConnection {
        TODO("not implemented")
    }
}
