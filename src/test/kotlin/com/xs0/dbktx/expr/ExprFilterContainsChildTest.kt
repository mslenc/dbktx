package com.xs0.dbktx.expr

import com.xs0.dbktx.conn.DbLoaderImpl
import com.xs0.dbktx.conn.RequestTime
import com.xs0.dbktx.schemas.test1.Brand.Companion.ITEMS_SET
import com.xs0.dbktx.schemas.test1.Item
import com.xs0.dbktx.schemas.test1.TestSchema1
import com.xs0.dbktx.util.testing.DelayedExec
import com.xs0.dbktx.util.testing.MockSQLConnection
import com.xs0.dbktx.util.defer
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.ext.sql.*
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Test

import java.util.Arrays
import java.util.concurrent.atomic.AtomicBoolean

import org.junit.Assert.*

class ExprFilterContainsChildTest {
    @Test
    fun testChildQuery() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: JsonArray? = null

        val connection = object : MockSQLConnection() {
            override fun queryWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
                theSql = sql
                theParams = params

                resultHandler.handle(Future.succeededFuture(ResultSet()))

                called.set(true)
                return this
            }
        }
        val delayedExec = DelayedExec()
        val db = DbLoaderImpl(connection, delayedExec, RequestTime.forTesting())

        db.run { defer {
            TestSchema1.BRAND.query {
                ITEMS_SET.contains {
                    Item.NAME oneOf setOf("item1", "item2")
                }
            }
        } }

        assertFalse(called.get())
        delayedExec.executePending()
        assertTrue(called.get())

        assertEquals("SELECT B.company_id, B.key, B.name, B.tag_line, B.t_created, B.t_updated FROM brands AS B WHERE (B.key, B.company_id) IN (SELECT I.brand_key, I.company_id FROM items AS I WHERE I.name IN (?, ?))", theSql)

        assertEquals(2, theParams!!.size())

        val strings = arrayOf<String>(theParams!!.getString(0), theParams!!.getString(1))
        Arrays.sort(strings)
        assertEquals("item1", strings[0])
        assertEquals("item2", strings[1])
    }
}