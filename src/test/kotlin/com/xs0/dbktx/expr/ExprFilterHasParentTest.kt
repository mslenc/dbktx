package com.xs0.dbktx.expr

import com.xs0.dbktx.conn.DbLoaderImpl
import com.xs0.dbktx.schemas.test1.Brand.TABLE.COMPANY_REF
import com.xs0.dbktx.schemas.test1.Company
import com.xs0.dbktx.schemas.test1.TestSchema1
import com.xs0.dbktx.util.DelayedExec
import com.xs0.dbktx.util.MockSQLConnection
import com.xs0.dbktx.util.defer
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.SQLConnection
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Test

import java.util.concurrent.atomic.AtomicBoolean

import org.junit.Assert.*

class ExprFilterHasParentTest {
    @Test
    fun testParentQuery() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: JsonArray? = null

        val connection = object : MockSQLConnection() {
            override fun queryWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
                called.set(true)
                theSql = sql
                theParams = params

                return this
            }
        }

        val delayedExec = DelayedExec()
        val db = DbLoaderImpl(connection, delayedExec)

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

        assertEquals(1, theParams!!.size())
        assertEquals("qwe", theParams!!.getString(0))

    }

    @Test
    fun testParentQueryInPresenceOfJoins() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: JsonArray? = null

        val connection = object : MockSQLConnection() {
            override fun queryWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
                called.set(true)
                theSql = sql
                theParams = params

                return this
            }
        }

        val delayedExec = DelayedExec()
        val db = DbLoaderImpl(connection, delayedExec)

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

        assertEquals("SELECT B.company_id, B.key, B.name, B.tag_line, B.t_created, B.t_updated FROM brands AS B INNER JOIN companies AS C ON B.company_id = C.id WHERE (C.name >= ?) ORDER BY C.name", theSql)

        assertEquals(1, theParams!!.size())
        assertEquals("qwe", theParams!!.getString(0))

    }
}