package com.github.mslenc.dbktx.expr

import com.github.mslenc.asyncdb.DbResultSet
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.schemas.test1.Brand.Companion.COMPANY_REF
import com.github.mslenc.dbktx.schemas.test1.Company
import com.github.mslenc.dbktx.schemas.test1.TestSchema1
import com.github.mslenc.dbktx.util.testing.DelayedExec
import com.github.mslenc.dbktx.util.testing.MockDbConnection
import com.github.mslenc.dbktx.util.testing.MockResultSet
import com.github.mslenc.dbktx.util.vertxDefer
import com.github.mslenc.dbktx.util.vertxRunBlocking
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import kotlinx.coroutines.runBlocking
import org.junit.Test

import java.util.concurrent.atomic.AtomicBoolean

import org.junit.Assert.*
import org.junit.Rule
import org.junit.runner.RunWith
import java.util.concurrent.CompletableFuture

@RunWith(VertxUnitRunner::class)
class ExprFilterHasParentTest {
    @Rule
    @JvmField
    var rule = RunTestOnContext()

    @Test
    fun testParentQuery() = vertxRunBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val connection = object : MockDbConnection() {
            override fun executeQuery(sql: String, values: MutableList<Any?>): CompletableFuture<DbResultSet> {
                called.set(true)
                theSql = sql
                theParams = values

                return CompletableFuture.completedFuture(MockResultSet.Builder().build())
            }
        }

        val delayedExec = DelayedExec()
        val db = DbLoaderImpl(connection, delayedExec, RequestTime.forTesting())

        val deferred = db.run { vertxDefer {
            TestSchema1.BRAND.query {
                COMPANY_REF.has {
                    Company.NAME gte "qwe"
                }
            }
        } }

        assertFalse(called.get())
        delayedExec.executePending()
        assertTrue(called.get())

        assertEquals("SELECT B.company_id, B.key, B.name, B.tag_line, B.t_created, B.t_updated FROM brands AS B WHERE B.company_id IN (SELECT C.id FROM companies AS C WHERE C.name >= ?)", theSql)

        assertEquals(1, theParams.size)
        assertEquals("qwe", theParams[0])

    }

    @Test
    fun testParentQueryInPresenceOfJoins() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val connection = object : MockDbConnection() {
            override fun executeQuery(sql: String, values: MutableList<Any?>): CompletableFuture<DbResultSet> {
                called.set(true)
                theSql = sql
                theParams = values

                return CompletableFuture.completedFuture(MockResultSet.Builder().build())
            }
        }

        val delayedExec = DelayedExec()
        val db = DbLoaderImpl(connection, delayedExec, RequestTime.forTesting())

        val deferred = db.run { vertxDefer {
            val query = newQuery(TestSchema1.BRAND)
            query.filter {
                COMPANY_REF.has {
                    Company.NAME gte "qwe"
                }
            }
            query.orderBy(COMPANY_REF, Company.NAME)
            query.run()
        } }

        assertFalse(called.get())
        delayedExec.executePending()
        assertTrue(called.get())

        assertEquals("SELECT B.company_id, B.key, B.name, B.tag_line, B.t_created, B.t_updated FROM brands AS B INNER JOIN companies AS C ON C.id = B.company_id WHERE (C.name >= ?) ORDER BY C.name", theSql)

        assertEquals(1, theParams.size)
        assertEquals("qwe", theParams[0])

    }
}