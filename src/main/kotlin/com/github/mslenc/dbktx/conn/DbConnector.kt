package com.github.mslenc.dbktx.conn

import kotlinx.coroutines.CoroutineScope

interface DbConnector {
    /**
     * Connects to database, provides it to the block, then closes the connection.
     */
    suspend fun connect(scope: CoroutineScope, block: suspend (DbConn)->Unit)
}
