package com.github.mslenc.dbktx.util

import com.github.mslenc.asyncdb.*
import com.github.mslenc.utils.debug
import com.github.mslenc.utils.getLogger
import java.util.concurrent.CompletableFuture

fun <T> wrapForTiming(promise: CompletableFuture<T>, started: Long): CompletableFuture<T> {
    return promise.whenComplete { _, _ ->
        val ended = System.currentTimeMillis()
        DebuggingConnection.logger.debug { "(${ended - started} ms)\n\n" }
    }
}

fun wrapForTiming(observer: DbQueryResultObserver, started: Long): DbQueryResultObserver {
    var rows = 0

    return object : DbQueryResultObserver {
        override fun onNext(row: DbRow?) {
            try {
                rows++
            } finally {
                observer.onNext(row)
            }
        }

        override fun onError(t: Throwable?) {
            try {
                val ended = System.currentTimeMillis()
                DebuggingConnection.logger.debug { "(${ended - started} ms) - error after $rows rows\n\n" }
            } finally {
                observer.onError(t)
            }
        }

        override fun onCompleted() {
            try {
                val ended = System.currentTimeMillis()
                DebuggingConnection.logger.debug { "(${ended - started} ms) - success after $rows rows\n\n" }
            } finally {
                observer.onCompleted()
            }
        }
    }
}

class DebuggingPreparedStatement(private val stmt: DbPreparedStatement, private val sql: String) : DbPreparedStatement {
    override fun getParameters(): DbColumns {
        return stmt.parameters
    }

    override fun getColumns(): DbColumns {
        return stmt.columns
    }

    override fun streamQuery(observer: DbQueryResultObserver, args: List<Any?>) {
        logger.debug { "Streaming PS with params $args:\n$sql" }
        val started = System.currentTimeMillis()
        stmt.streamQuery(wrapForTiming(observer, started), args)
    }

    override fun streamQuery(streamHandler: DbQueryResultObserver, vararg values: Any?) {
        logger.debug { "Streaming PS with params ${values.toList()}:\n$sql" }
        val started = System.currentTimeMillis()
        stmt.streamQuery(wrapForTiming(streamHandler, started), *values)
    }

    override fun close(): CompletableFuture<Void> {
        return stmt.close()
    }

    override fun execute(args: List<Any?>): CompletableFuture<DbExecResult> {
        logger.debug { "Executing PS with params $args:\n$sql" }
        val started = System.currentTimeMillis()
        return wrapForTiming(stmt.execute(args), started)
    }

    override fun execute(vararg values: Any?): CompletableFuture<DbExecResult> {
        logger.debug { "Executing PS with params ${values.toList()}:\n$sql" }
        val started = System.currentTimeMillis()
        return wrapForTiming(stmt.execute(*values), started)
    }

    override fun executeQuery(values: List<Any?>): CompletableFuture<DbResultSet> {
        logger.debug { "Executing PS query with params $values:\n$sql" }
        val started = System.currentTimeMillis()
        return wrapForTiming(stmt.executeQuery(values), started)
    }

    override fun executeQuery(vararg values: Any?): CompletableFuture<DbResultSet> {
        logger.debug { "Executing PS query with params ${values.toList()}:\n$sql" }
        val started = System.currentTimeMillis()
        return wrapForTiming(stmt.executeQuery(*values), started)
    }

    override fun executeUpdate(values: List<Any?>): CompletableFuture<DbUpdateResult> {
        logger.debug { "Executing PS update with params $values:\n$sql" }
        val started = System.currentTimeMillis()
        return wrapForTiming(stmt.executeUpdate(values), started)
    }

    override fun executeUpdate(vararg values: Any?): CompletableFuture<DbUpdateResult> {
        logger.debug { "Executing PS update with params ${values.toList()}:\n$sql" }
        val started = System.currentTimeMillis()
        return wrapForTiming(stmt.executeUpdate(*values), started)
    }

    companion object {
        val logger = getLogger<DebuggingPreparedStatement>()
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
        val started = System.currentTimeMillis()
        return wrapForTiming(conn.execute(sql), started)
    }

    override fun execute(sql: String, params: List<Any?>): CompletableFuture<DbExecResult> {
        logger.debug { "Querying with params $params:\n$sql" }
        val started = System.currentTimeMillis()
        return wrapForTiming(conn.execute(sql, params), started)
    }

    override fun execute(sql: String, vararg params: Any?): CompletableFuture<DbExecResult> {
        logger.debug { "Querying with params ${ params.toList() }:\n$sql" }
        val started = System.currentTimeMillis()
        return wrapForTiming(conn.execute(sql, *params), started)
    }

    override fun executeQuery(sql: String): CompletableFuture<DbResultSet> {
        logger.debug { "Querying with no params:\n$sql" }
        val started = System.currentTimeMillis()
        return wrapForTiming(conn.executeQuery(sql), started)
    }

    override fun executeQuery(sql: String, values: List<Any?>): CompletableFuture<DbResultSet> {
        logger.debug { "Querying with params $values:\n$sql" }
        val started = System.currentTimeMillis()
        return wrapForTiming(conn.executeQuery(sql, values), started)
    }

    override fun executeQuery(sql: String, vararg values: Any?): CompletableFuture<DbResultSet> {
        logger.debug { "Querying with params ${ values.toList() }:\n$sql" }
        val started = System.currentTimeMillis()
        return wrapForTiming(conn.executeQuery(sql, *values), started)
    }

    override fun executeUpdate(sql: String): CompletableFuture<DbUpdateResult> {
        logger.debug { "Updating with no params:\n$sql" }
        val started = System.currentTimeMillis()
        return wrapForTiming(conn.executeUpdate(sql), started)
    }

    override fun executeUpdate(sql: String?, values: List<Any?>): CompletableFuture<DbUpdateResult> {
        logger.debug { "Updating with params $values:\n$sql" }
        val started = System.currentTimeMillis()
        return wrapForTiming(conn.executeUpdate(sql, values), started)
    }

    override fun executeUpdate(sql: String?, vararg values: Any?): CompletableFuture<DbUpdateResult> {
        logger.debug { "Querying with params ${ values.toList() }:\n$sql" }
        val started = System.currentTimeMillis()
        return wrapForTiming(conn.executeUpdate(sql, *values), started)
    }

    override fun prepareStatement(sql: String): CompletableFuture<DbPreparedStatement> {
        return conn.prepareStatement(sql).thenApply { DebuggingPreparedStatement(it, sql) }
    }

    override fun streamQuery(sql: String, observer: DbQueryResultObserver) {
        logger.debug { "Streaming with no params:\n$sql" }
        val started = System.currentTimeMillis()
        conn.streamQuery(sql, wrapForTiming(observer, started))
    }

    override fun streamQuery(sql: String, observer: DbQueryResultObserver, args: List<Any?>) {
        logger.debug { "Streaming with params $args:\n$sql" }
        val started = System.currentTimeMillis()
        conn.streamQuery(sql, wrapForTiming(observer, started), args)
    }

    override fun streamQuery(sql: String, streamHandler: DbQueryResultObserver, vararg values: Any?) {
        logger.debug { "Streaming with params ${values.toList()}:\n$sql" }
        val started = System.currentTimeMillis()
        conn.streamQuery(sql, wrapForTiming(streamHandler, started), *values)
    }

    override fun getConfig(): DbConfig {
        return conn.config
    }

    override fun close(): CompletableFuture<Void> {
        return conn.close()
    }

    companion object {
        val logger = getLogger<DebuggingConnection>()
    }
}
