package com.xs0.dbktx.expr

import com.xs0.dbktx.conn.DbLoaderImpl
import com.xs0.dbktx.schemas.test1.Item
import com.xs0.dbktx.schemas.test1.TestSchema1
import com.xs0.dbktx.util.DelayedExec
import com.xs0.dbktx.util.MockSQLConnection
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.ext.sql.*
import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.launch
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
        val db = DbLoaderImpl(connection, delayedExec)

        db.run {
            TestSchema1.BRAND.queryAsync {
                ITEMS_SET.contains(TestSchema1.ITEM) {
                    Item.NAME oneOf setOf("item1", "item2")
                }
            }
        }

        assertTrue(called.get())

        assertEquals("SELECT company_id, key, name, tag_line, t_created, t_updated FROM brands WHERE (key, company_id) IN (SELECT brand_key, company_id FROM items WHERE name IN (?, ?))", theSql)

        assertEquals(2, theParams!!.size())

        val strings = arrayOf<String>(theParams!!.getString(0), theParams!!.getString(1))
        Arrays.sort(strings)
        assertEquals("item1", strings[0])
        assertEquals("item2", strings[1])
    }
}