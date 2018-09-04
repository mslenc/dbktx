package com.xs0.dbktx.expr

import com.xs0.asyncdb.common.ResultSet
import com.xs0.asyncdb.vertx.DbConnection
import com.xs0.dbktx.conn.DbLoaderImpl
import com.xs0.dbktx.conn.RequestTime
import com.xs0.dbktx.schemas.test1.Brand.Companion.COMPANY_REF
import com.xs0.dbktx.schemas.test1.Company
import com.xs0.dbktx.schemas.test1.TestSchema1
import com.xs0.dbktx.util.testing.DelayedExec
import com.xs0.dbktx.util.defer
import com.xs0.dbktx.util.testing.MockDbConnection
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Test

import java.util.concurrent.atomic.AtomicBoolean

import org.junit.Assert.*

class ExprFilterHasParentTest {
    @Test
    fun testParentQuery() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any> = emptyList()

        val connection = object : MockDbConnection() {
            override fun queryWithParams(sql: String, params: List<Any>, resultHandler: Handler<AsyncResult<ResultSet>>): DbConnection {
                called.set(true)
                theSql = sql
                theParams = params

                return this
            }
        }

        val delayedExec = DelayedExec()
        val db = DbLoaderImpl(connection, delayedExec, RequestTime.forTesting())

        val deferred = db.run { defer {
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
        var theParams: List<Any> = emptyList()

        val connection = object : MockDbConnection() {
            override fun queryWithParams(sql: String, params: List<Any>, resultHandler: Handler<AsyncResult<ResultSet>>): DbConnection {
                called.set(true)
                theSql = sql
                theParams = params

                return this
            }
        }

        val delayedExec = DelayedExec()
        val db = DbLoaderImpl(connection, delayedExec, RequestTime.forTesting())

        val deferred = db.run { defer {
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