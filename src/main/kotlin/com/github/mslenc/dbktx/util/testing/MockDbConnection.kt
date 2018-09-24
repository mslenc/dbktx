package com.github.mslenc.dbktx.util.testing

import com.github.mslenc.asyncdb.common.ResultSet
import com.github.mslenc.asyncdb.vertx.DbConnection
import com.github.mslenc.asyncdb.vertx.TransactionIsolation
import com.github.mslenc.asyncdb.vertx.UpdateResult
import io.vertx.core.AsyncResult
import io.vertx.core.Handler

open class MockDbConnection : DbConnection {
    override fun updateWithParams(sql: String, params: List<Any>, resultHandler: Handler<AsyncResult<UpdateResult>>): DbConnection {
        TODO("not implemented")
    }

    override fun rollback(handler: Handler<AsyncResult<Void>>): DbConnection {
        TODO("not implemented")
    }

    override fun query(sql: String, resultHandler: Handler<AsyncResult<ResultSet>>): DbConnection {
        TODO("not implemented")
    }

    override fun commit(handler: Handler<AsyncResult<Void>>): DbConnection {
        TODO("not implemented")
    }

    override fun execute(sql: String, resultHandler: Handler<AsyncResult<Void>>): DbConnection {
        TODO("not implemented")
    }

    override fun setTransactionIsolation(isolation: TransactionIsolation, handler: Handler<AsyncResult<Void>>): DbConnection {
        TODO("not implemented")
    }

    override fun setAutoCommit(autoCommit: Boolean, resultHandler: Handler<AsyncResult<Void>>): DbConnection {
        TODO("not implemented")
    }

    override fun queryWithParams(sql: String, params: List<Any>, resultHandler: Handler<AsyncResult<ResultSet>>): DbConnection {
        TODO("not implemented")
    }

    override fun update(sql: String, resultHandler: Handler<AsyncResult<UpdateResult>>): DbConnection {
        TODO("not implemented")
    }

    override fun close(handler: Handler<AsyncResult<Void>>) {
        TODO("not implemented")
    }

    override fun close() {
        TODO("not implemented")
    }
}
