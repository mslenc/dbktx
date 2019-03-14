package com.github.mslenc.dbktx.util

import com.github.mslenc.asyncdb.*
import mu.KLogging
import java.util.concurrent.CompletableFuture

fun <T> wrapForTiming(promise: CompletableFuture<T>): CompletableFuture<T> {
    val started = System.currentTimeMillis()
    return promise.whenComplete { _, _ ->
        val ended = System.currentTimeMillis()
        DebuggingConnection.logger.debug { "(${ended - started} ms)\n\n" }
    }
}

fun wrapForTiming(observer: DbQueryResultObserver): DbQueryResultObserver {
    val started = System.currentTimeMillis()
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
        stmt.streamQuery(wrapForTiming(observer), args)
    }

    override fun streamQuery(streamHandler: DbQueryResultObserver, vararg values: Any?) {
        logger.debug { "Streaming PS with params ${values.toList()}:\n$sql" }
        stmt.streamQuery(wrapForTiming(streamHandler), *values)
    }

    override fun close(): CompletableFuture<Void> {
        return stmt.close()
    }

    override fun execute(args: List<Any?>): CompletableFuture<DbExecResult> {
        logger.debug { "Executing PS with params $args:\n$sql" }
        return wrapForTiming(stmt.execute(args))
    }

    override fun execute(vararg values: Any?): CompletableFuture<DbExecResult> {
        logger.debug { "Executing PS with params ${values.toList()}:\n$sql" }
        return wrapForTiming(stmt.execute(*values))
    }

    override fun executeQuery(values: List<Any?>): CompletableFuture<DbResultSet> {
        logger.debug { "Executing PS query with params $values:\n$sql" }
        return wrapForTiming(stmt.executeQuery(values))
    }

    override fun executeQuery(vararg values: Any?): CompletableFuture<DbResultSet> {
        logger.debug { "Executing PS query with params ${values.toList()}:\n$sql" }
        return wrapForTiming(stmt.executeQuery(*values))
    }

    override fun executeUpdate(values: List<Any?>): CompletableFuture<DbUpdateResult> {
        logger.debug { "Executing PS update with params $values:\n$sql" }
        return wrapForTiming(stmt.executeUpdate(values))
    }

    override fun executeUpdate(vararg values: Any?): CompletableFuture<DbUpdateResult> {
        logger.debug { "Executing PS update with params ${values.toList()}:\n$sql" }
        return wrapForTiming(stmt.executeUpdate(*values))
    }

    companion object : KLogging()
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

    override fun executeQuery(sql: String): CompletableFuture<DbResultSet> {
        logger.debug { "Querying with no params:\n$sql" }
        return wrapForTiming(conn.executeQuery(sql))
    }

    override fun executeQuery(sql: String, values: List<Any?>): CompletableFuture<DbResultSet> {
        logger.debug { "Querying with params $values:\n$sql" }
        return wrapForTiming(conn.executeQuery(sql, values))
    }

    override fun executeQuery(sql: String, vararg values: Any?): CompletableFuture<DbResultSet> {
        logger.debug { "Querying with params ${ values.toList() }:\n$sql" }
        return wrapForTiming(conn.executeQuery(sql, *values))
    }

    override fun executeUpdate(sql: String): CompletableFuture<DbUpdateResult> {
        logger.debug { "Updating with no params:\n$sql" }
        return wrapForTiming(conn.executeUpdate(sql))
    }

    override fun executeUpdate(sql: String?, values: List<Any?>): CompletableFuture<DbUpdateResult> {
        logger.debug { "Updating with params $values:\n$sql" }
        return wrapForTiming(conn.executeUpdate(sql, values))
    }

    override fun executeUpdate(sql: String?, vararg values: Any?): CompletableFuture<DbUpdateResult> {
        logger.debug { "Querying with params ${ values.toList() }:\n$sql" }
        return wrapForTiming(conn.executeUpdate(sql, *values))
    }

    override fun prepareStatement(sql: String): CompletableFuture<DbPreparedStatement> {
        return conn.prepareStatement(sql).thenApply { DebuggingPreparedStatement(it, sql) }
    }

    override fun streamQuery(sql: String, observer: DbQueryResultObserver) {
        logger.debug { "Streaming with no params:\n$sql" }
        conn.streamQuery(sql, wrapForTiming(observer))
    }

    override fun streamQuery(sql: String, observer: DbQueryResultObserver, args: List<Any?>) {
        logger.debug { "Streaming with params $args:\n$sql" }
        conn.streamQuery(sql, wrapForTiming(observer), args)
    }

    override fun streamQuery(sql: String?, streamHandler: DbQueryResultObserver?, vararg values: Any?) {
        logger.debug { "Streaming with params ${values.toList()}:\n$sql" }
        conn.streamQuery(sql, streamHandler, *values)
    }

    override fun getConfig(): DbConfig {
        return conn.config
    }

    override fun close(): CompletableFuture<Void> {
        return conn.close()
    }

    companion object : KLogging()
}
