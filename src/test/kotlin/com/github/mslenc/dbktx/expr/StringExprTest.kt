package com.github.mslenc.dbktx.expr

import com.github.mslenc.asyncdb.DbExecResult
import com.github.mslenc.asyncdb.impl.DbQueryResultImpl
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.conn.newUpdate
import com.github.mslenc.dbktx.crud.dsl.concatWs
import com.github.mslenc.dbktx.schemas.initSchemas
import com.github.mslenc.dbktx.schemas.test1.Brand
import com.github.mslenc.dbktx.util.testing.MockDbConnection
import kotlinx.coroutines.runBlocking
import org.junit.Test

import java.util.concurrent.atomic.AtomicBoolean

import org.junit.Assert.*
import java.util.*
import java.util.concurrent.CompletableFuture

class StringExprTest {
    init {
        initSchemas()
    }

    @Test
    fun testConcatWS() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val companyId = UUID.randomUUID()

        val connection = object : MockDbConnection() {
            override fun execute(sql: String, args: List<Any?>): CompletableFuture<DbExecResult> {
                called.set(true)
                theSql = sql
                theParams = args

                return CompletableFuture.completedFuture(DbQueryResultImpl(3, "OK", null, null))
            }
        }

        val db = DbLoaderImpl(connection, this, RequestTime.forTesting())

        val newInfo = "abc"

        with(Brand) {
            val update = newUpdate(db)
            update.filter {
                COMPANY_ID eq companyId
            }

            update[NAME] becomes { concatWs(":", +NAME, literal(newInfo), +TAG_LINE) }

            update.execute()
        }

        assertTrue(called.get())

        assertEquals("UPDATE \"brands\" SET \"name\"=CONCAT_WS(?, \"name\", ?, \"tag_line\") WHERE \"company_id\" = ?", theSql)

        assertEquals(3, theParams.size)
        assertEquals(":", theParams[0])
        assertEquals("abc", theParams[1])
        assertEquals(companyId.toString(), theParams[2])
    }

    @Test
    fun testConcatWSOnNullableColumn() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val companyId = UUID.randomUUID()

        val connection = object : MockDbConnection() {
            override fun execute(sql: String, args: List<Any?>): CompletableFuture<DbExecResult> {
                called.set(true)
                theSql = sql
                theParams = args

                return CompletableFuture.completedFuture(DbQueryResultImpl(3, "OK", null, null))
            }
        }

        val db = DbLoaderImpl(connection, this, RequestTime.forTesting())

        val newInfo = "qwe"

        with(Brand) {
            val update = newUpdate(db)
            update.filter {
                COMPANY_ID eq companyId
            }

            update[TAG_LINE] becomes { concatWs(":", +NAME, literal(newInfo), +TAG_LINE) }

            update.execute()
        }

        assertTrue(called.get())

        assertEquals("UPDATE \"brands\" SET \"tag_line\"=CONCAT_WS(?, \"name\", ?, \"tag_line\") WHERE \"company_id\" = ?", theSql)

        assertEquals(3, theParams.size)
        assertEquals(":", theParams[0])
        assertEquals("qwe", theParams[1])
        assertEquals(companyId.toString(), theParams[2])
    }
}