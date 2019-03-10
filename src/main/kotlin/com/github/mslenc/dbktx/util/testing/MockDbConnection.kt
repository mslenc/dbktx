package com.github.mslenc.dbktx.util.testing

import com.github.mslenc.asyncdb.*
import java.util.concurrent.CompletableFuture

open class MockDbConnection(dbType: DbType = DbType.POSTGRES) : DbConnection {
    private val dbConfig: DbConfig

    init {
        dbConfig = DbConfig.newBuilder(dbType).build()
    }

    override fun execute(sql: String): CompletableFuture<DbExecResult> {
        TODO("not implemented")
    }

    override fun execute(sql: String, args: List<Any?>): CompletableFuture<DbExecResult> {
        TODO("not implemented")
    }

    override fun prepareStatement(sql: String): CompletableFuture<DbPreparedStatement> {
        TODO("not implemented")
    }

    override fun streamQuery(sql: String, observer: DbQueryResultObserver) {
        TODO("not implemented")
    }

    override fun streamQuery(sql: String, observer: DbQueryResultObserver, args: List<Any?>) {
        TODO("not implemented")
    }

    override fun getConfig(): DbConfig {
        return dbConfig
    }

    override fun close(): CompletableFuture<Void> {
        TODO("not implemented")
    }
}
