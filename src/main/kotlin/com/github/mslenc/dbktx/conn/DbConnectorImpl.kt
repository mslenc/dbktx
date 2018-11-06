package com.github.mslenc.dbktx.conn

import com.github.mslenc.asyncdb.DbDataSource
import com.github.mslenc.asyncdb.DbConnection
import com.github.mslenc.dbktx.util.DelayedExecScheduler
import io.vertx.core.Vertx
import kotlinx.coroutines.future.await
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
        private val sqlClient: DbDataSource,
        private val delayedExecScheduler: DelayedExecScheduler = VertxScheduler,
        private val timeProvider: TimeProvider,
        private val connectionWrapper: (DbConnection)->DbConnection = { it }
        )
    : DbConnector

{
    override suspend fun connect(block: suspend (DbConn) -> Unit) {
        val rawConn: DbConnection = try {
            sqlClient.connect().await()
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
            rawConn.close()
        }
    }

    companion object : KLogging()
}