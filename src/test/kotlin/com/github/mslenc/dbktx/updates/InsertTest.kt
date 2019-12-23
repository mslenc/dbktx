package com.github.mslenc.dbktx.updates

import com.github.mslenc.asyncdb.DbExecResult
import com.github.mslenc.asyncdb.impl.DbQueryResultImpl
import com.github.mslenc.asyncdb.util.GeneratedIdResult
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.conn.insert
import com.github.mslenc.dbktx.schemas.test1.*
import com.github.mslenc.dbktx.util.testing.MockDbConnection
import kotlinx.coroutines.runBlocking
import org.junit.Test

import java.util.concurrent.atomic.AtomicBoolean

import org.junit.Assert.*
import java.util.*
import java.util.concurrent.CompletableFuture

class InsertTest {
    init {
        TestSchema1.numberOfTables // init
    }

    @Test
    fun testInsert1() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val newPurchaseId = 1817512L

        val connection = object : MockDbConnection() {
            override fun execute(sql: String, args: List<Any?>): CompletableFuture<DbExecResult> {
                called.set(true)
                theSql = sql
                theParams = args

                val result = DbQueryResultImpl(1L, null, null, GeneratedIdResult(newPurchaseId))
                return CompletableFuture.completedFuture(result)
            }
        }

        val now = RequestTime.forTesting()
        val companyId = UUID.randomUUID()
        val db = DbLoaderImpl(connection, this, now)

        val purchaseId = Purchase.insert(db) {
            it[COMPANY_ID] = companyId
            it[T_CREATED] = now.localDateTime
            it[T_UPDATED] = now.localDateTime
        }

        assertTrue(called.get())

        assertEquals("INSERT INTO \"purchases\"(\"company_id\", \"t_created\", \"t_updated\") VALUES (?, '${now.localDateTime}', '${now.localDateTime}') RETURNING \"id\"", theSql)

        assertEquals(1, theParams.size)
        assertEquals(companyId.toString(), theParams[0])

        assertEquals(newPurchaseId, purchaseId)
    }

    @Test
    fun testInsert2() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()


        val connection = object : MockDbConnection() {
            override fun execute(sql: String, args: List<Any?>): CompletableFuture<DbExecResult> {
                called.set(true)
                theSql = sql
                theParams = args

                val result = DbQueryResultImpl(1L, null, null, null)
                return CompletableFuture.completedFuture(result)
            }
        }

        val now = RequestTime.forTesting()
        val companyId = UUID.randomUUID()
        val newPurchaseId = 5235345L
        val db = DbLoaderImpl(connection, this, now)

        val purchaseId = Purchase.insert(db) {
            it[ID] = 5235345L
            it[COMPANY_ID] = companyId
            it[T_CREATED] = now.localDateTime
            it[T_UPDATED] = now.localDateTime
        }

        assertTrue(called.get())

        assertEquals("INSERT INTO \"purchases\"(\"id\", \"company_id\", \"t_created\", \"t_updated\") VALUES ($newPurchaseId, ?, '${now.localDateTime}', '${now.localDateTime}')", theSql)

        assertEquals(1, theParams.size)
        assertEquals(companyId.toString(), theParams[0])

        assertEquals(newPurchaseId, purchaseId)
    }
}