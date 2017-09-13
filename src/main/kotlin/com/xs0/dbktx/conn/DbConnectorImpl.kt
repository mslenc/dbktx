package com.xs0.dbktx.conn

import com.xs0.dbktx.util.DelayedExecScheduler
import com.xs0.dbktx.util.vx
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
    suspend override fun connect(block: suspend (DbConn) -> Unit) {
        val rawConn: SQLConnection = try {
            vx { handler -> sqlClient.getConnection(handler) }
        } catch (e: Exception) {
            logger.error("Failed to connect", e)
            throw e
        }

        DbLoaderImpl(rawConn, delayedExecScheduler).use {
            block(it)
        }
    }

    companion object : KLogging()
}