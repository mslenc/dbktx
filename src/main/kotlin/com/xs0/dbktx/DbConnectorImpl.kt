package com.xs0.dbktx

import io.vertx.core.Vertx
import io.vertx.ext.asyncsql.AsyncSQLClient
import io.vertx.ext.sql.SQLConnection
import mu.KLogging

object VertxScheduler: DelayedExecScheduler {
    override fun schedule(runnable: () -> Unit) {
        Vertx.currentContext().runOnContext({ runnable() })
    }
}

/**
 * Creates a connector that obtains connections from the provided sql client and
 * schedules delayed execution on the provided scheduler. Expected to be used
 * mostly for testing.
 */
class DbConnectorImpl(
        private val sqlClient: AsyncSQLClient,
        private val delayedExecScheduler: DelayedExecScheduler = VertxScheduler)
    : DbConnector

{
    override suspend fun connect(): DbConn {
        val rawConn: SQLConnection = try {
            vx { handler -> sqlClient.getConnection(handler) }
        } catch (e: Exception) {
            logger.error("Failed to connect", e)
            throw e
        }

        return DbLoaderImpl(rawConn, delayedExecScheduler)
    }

    fun rawClient(): AsyncSQLClient {
        return sqlClient
    }

    companion object : KLogging()
}