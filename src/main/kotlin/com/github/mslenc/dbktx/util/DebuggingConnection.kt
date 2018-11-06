package com.github.mslenc.dbktx.util

import com.github.mslenc.asyncdb.*
import mu.KLogging
import java.util.concurrent.CompletableFuture

fun <T> wrapForTiming(promise: CompletableFuture<T>): CompletableFuture<T> {
    val started = System.currentTimeMillis()
    return promise.whenComplete { t, u ->
        val ended = System.currentTimeMillis()
        DebuggingConnection.logger.debug { "(${ended - started} ms)\n\n" }
    }
}

class DebuggingPreparedStatement(private val stmt: DbPreparedStatement) : DbPreparedStatement {
    override fun getParameters(): DbColumns {
        return stmt.parameters
    }

    override fun getColumns(): DbColumns {
        return stmt.columns
    }

    override fun streamQuery(observer: DbQueryResultObserver, args: List<Any?>) {
        return stmt.streamQuery(observer, args)
    }

    override fun close(): CompletableFuture<Void> {
        return stmt.close()
    }

    override fun execute(args: List<Any?>): CompletableFuture<DbExecResult> {
        return stmt.execute(args)
    }
}

class DebuggingConnection(private val conn: DbConnection) : DbConnection {
    override fun startTransaction(): CompletableFuture<Void> {
        return conn.startTransaction()
    }

    override fun startTransaction(isolation: DbTxIsolation?): CompletableFuture<Void> {
        return conn.startTransaction(isolation)
    }

    override fun startTransaction(mode: DbTxMode?): CompletableFuture<Void> {
        return conn.startTransaction(mode)
    }

    override fun startTransaction(isolation: DbTxIsolation?, mode: DbTxMode?): CompletableFuture<Void> {
        return conn.startTransaction(isolation, mode)
    }

    override fun commit(): CompletableFuture<Void> {
        return conn.commit()
    }

    override fun rollback(): CompletableFuture<Void> {
        return conn.rollback()
    }

    override fun commitAndChain(): CompletableFuture<Void> {
        return conn.commitAndChain()
    }

    override fun rollbackAndChain(): CompletableFuture<Void> {
        return conn.rollbackAndChain()
    }

    override fun execute(sql: String): CompletableFuture<DbExecResult> {
        logger.debug { "Querying with no params:\n$sql" }
        return wrapForTiming(conn.execute(sql))
    }

    override fun execute(sql: String, params: List<Any?>): CompletableFuture<DbExecResult> {
        logger.debug { "Querying with params $params:\n$sql" }
        return wrapForTiming(conn.execute(sql, params))
    }

    override fun execute(sql: String, vararg params: Any?): CompletableFuture<DbExecResult> {
        logger.debug { "Querying with params ${ params.toList() }:\n$sql" }
        return wrapForTiming(conn.execute(sql, *params))
    }

    override fun prepareStatement(sql: String): CompletableFuture<DbPreparedStatement> {
        return conn.prepareStatement(sql).thenApply { DebuggingPreparedStatement(it) }
    }

    override fun streamQuery(sql: String, observer: DbQueryResultObserver) {
        conn.streamQuery(sql, observer)
    }

    override fun streamQuery(sql: String, observer: DbQueryResultObserver, args: List<Any?>) {
        conn.streamQuery(sql, observer, args)
    }

    override fun getConfig(): DbConfig {
        return conn.config
    }

    override fun close(): CompletableFuture<Void> {
        return conn.close()
    }

    companion object : KLogging()
}
