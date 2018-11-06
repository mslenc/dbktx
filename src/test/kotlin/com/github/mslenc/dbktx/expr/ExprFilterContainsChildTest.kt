package com.github.mslenc.dbktx.expr

import com.github.mslenc.asyncdb.DbResultSet
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.schemas.test1.Brand.Companion.ITEMS_SET
import com.github.mslenc.dbktx.schemas.test1.Item
import com.github.mslenc.dbktx.schemas.test1.TestSchema1
import com.github.mslenc.dbktx.util.testing.DelayedExec
import com.github.mslenc.dbktx.util.defer
import com.github.mslenc.dbktx.util.testing.MockDbConnection
import com.github.mslenc.dbktx.util.testing.MockResultSet
import com.github.mslenc.dbktx.util.vertxDispatcher
import io.vertx.ext.unit.TestContext
import io.vertx.ext.unit.junit.VertxUnitRunner
import kotlinx.coroutines.runBlocking
import org.junit.Test

import java.util.Arrays
import java.util.concurrent.atomic.AtomicBoolean

import org.junit.Assert.*
import org.junit.runner.RunWith
import java.util.concurrent.CompletableFuture
import io.vertx.ext.unit.junit.RunTestOnContext
import org.junit.Rule


@RunWith(VertxUnitRunner::class)
class ExprFilterContainsChildTest {
    @Rule
    @JvmField
    var rule = RunTestOnContext()

    @Test
    fun testChildQuery(ctx: TestContext) = runBlocking(vertxDispatcher()) {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val connection = object : MockDbConnection() {
            override fun executeQuery(sql: String, values: MutableList<Any?>): CompletableFuture<DbResultSet> {
                theSql = sql
                theParams = values
                called.set(true)
                return CompletableFuture.completedFuture(MockResultSet.Builder().build())
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

        assertEquals(2, theParams.size)

        val strings = arrayOf(theParams[0] as String, theParams[1] as String)
        Arrays.sort(strings)
        assertEquals("item1", strings[0])
        assertEquals("item2", strings[1])
    }
}