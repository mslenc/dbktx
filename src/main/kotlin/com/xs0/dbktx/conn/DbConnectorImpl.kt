package com.xs0.dbktx.conn

import com.xs0.asyncdb.vertx.DbClient
import com.xs0.asyncdb.vertx.DbConnection
import com.xs0.dbktx.util.DelayedExecScheduler
import com.xs0.dbktx.util.vx
import io.vertx.core.Vertx
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
        private val sqlClient: DbClient,
        private val delayedExecScheduler: DelayedExecScheduler = VertxScheduler,
        private val timeProvider: TimeProvider,
        private val connectionWrapper: (DbConnection)->DbConnection = { it }
        )
    : DbConnector

{
    override suspend fun connect(block: suspend (DbConn) -> Unit) {
        val rawConn: DbConnection = try {
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