package com.xs0.dbktx

interface DbConnector {
    suspend fun connect(): DbConn
}
