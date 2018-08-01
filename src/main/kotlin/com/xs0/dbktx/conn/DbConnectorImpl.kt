package com.xs0.dbktx.conn

import com.xs0.dbktx.util.DelayedExecScheduler
import com.xs0.dbktx.util.vx
import io.vertx.core.Vertx
import io.vertx.ext.asyncsql.AsyncSQLClient
import io.vertx.ext.sql.SQLConnection
import mu.KLogging

object VertxScheduler: DelayedExecScheduler {
    override fun schedule(runnable: () -> Unit) {
        Vertx.currentContext().runOnContext { runnable() }
    }
}

/**
 * Creates a connector that obtains connections from the provided sql client and
 * schedules delayed execution on the provided scheduler.
 */
class DbConnectorImpl(
        private val sqlClient: AsyncSQLClient,
        private val delayedExecScheduler: DelayedExecScheduler = VertxScheduler,
        private val timeProvider: TimeProvider,
        private val connectionWrapper: (SQLConnection)->SQLConnection = { it }
        )
    : DbConnector

{
    override suspend fun connect(block: suspend (DbConn) -> Unit) {
        val rawConn: SQLConnection = try {
            vx { handler -> sqlClient.getConnection(handler) }
        } catch (e: Exception) {
            logger.error("Failed to connect", e)
            throw e
        }

        try {
            val wrappedConn = connectionWrapper(rawConn)
            val requestTime = timeProvider.getTime(wrappedConn)
            val dbConn = DbLoaderImpl(wrappedConn, delayedExecScheduler, requestTime)

            block(dbConn)
        } finally {
            vx<Void> { handler -> rawConn.close(handler) }
        }
    }

    companion object : KLogging()
}