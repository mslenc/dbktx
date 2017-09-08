package com.xs0.dbktx.conn

interface DbConnector {
    suspend fun connect(): DbConn
}
