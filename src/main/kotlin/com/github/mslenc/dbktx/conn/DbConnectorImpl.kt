package com.github.mslenc.dbktx.conn

import com.github.mslenc.asyncdb.DbDataSource
import com.github.mslenc.asyncdb.DbConnection
import com.github.mslenc.dbktx.util.makeDbContext
import com.github.mslenc.utils.getLogger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext

/**
 * Creates a connector that obtains connections from the provided sql client and
 * schedules delayed execution on the provided scheduler.
 */
class DbConnectorImpl(
        private val sqlClient: DbDataSource,
        private val timeProvider: TimeProvider,
        private val connectionWrapper: (DbConnection)->DbConnection = { it }
        )
    : DbConnector

{
    override suspend fun connect(scope: CoroutineScope, block: suspend (DbConn) -> Unit) {
        val rawConn: DbConnection = try {
            sqlClient.connect().await()
        } catch (e: Throwable) {
            logger.error("Failed to connect", e)
            throw e
        }

        try {
            val wrappedConn = connectionWrapper(rawConn)
            val requestTime = timeProvider.getTime(wrappedConn)
            val dbConn = DbLoaderImpl(wrappedConn, scope, requestTime)

            withContext(makeDbContext(dbConn)) {
                block(dbConn)
            }
        } finally {
            rawConn.close()
        }
    }

    companion object {
        val logger = getLogger<DbConnectorImpl>()
    }
}