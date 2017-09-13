package com.xs0.dbktx.conn

interface DbConnector {
    /**
     * Connects to database, provides it to the block, then closes the connection.
     */
    suspend fun connect(block: suspend (DbConn)->Unit)
}
