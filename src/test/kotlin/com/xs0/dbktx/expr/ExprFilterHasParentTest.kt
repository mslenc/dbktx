package com.xs0.dbktx.expr

import com.xs0.dbktx.conn.DbLoaderImpl
import com.xs0.dbktx.schemas.test1.TestSchema1
import com.xs0.dbktx.util.DelayedExec
import com.xs0.dbktx.util.MockSQLConnection
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.SQLConnection
import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.launch
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

        val deferred = db.run {
            TestSchema1.BRAND.queryAsync {
                COMPANY_REF.has(TestSchema1.COMPANY) {
                    NAME gte "qwe"
                }
            }
        }

        assertFalse(called.get())
        delayedExec.executePending()
        assertTrue(called.get())

        assertEquals("SELECT company_id, key, name, tag_line, t_created, t_updated FROM brands WHERE company_id IN (SELECT id FROM companies WHERE name >= ?)", theSql)

        assertEquals(1, theParams!!.size())
        assertEquals("qwe", theParams!!.getString(0))

    }
}