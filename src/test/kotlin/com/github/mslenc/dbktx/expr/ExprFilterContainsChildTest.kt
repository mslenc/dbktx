package com.github.mslenc.dbktx.expr

import com.github.mslenc.asyncdb.DbResultSet
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.schemas.test1.Brand.Companion.ITEMS_SET
import com.github.mslenc.dbktx.schemas.test1.Item
import com.github.mslenc.dbktx.schemas.test1.TestSchema1
import com.github.mslenc.dbktx.util.testing.MockDbConnection
import com.github.mslenc.dbktx.util.testing.MockResultSet
import org.junit.Test

import java.util.Arrays
import java.util.concurrent.atomic.AtomicBoolean

import org.junit.Assert.*
import java.util.concurrent.CompletableFuture
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking


class ExprFilterContainsChildTest {
    @Test
    fun testChildQuery() = runBlocking {
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

        val db = DbLoaderImpl(connection, this, RequestTime.forTesting())

        val deferred = db.run { async {
            TestSchema1.BRAND.query {
                ITEMS_SET.contains {
                    Item.NAME oneOf setOf("item1", "item2")
                }
            }
        } }

        assertFalse(called.get())

        deferred.await()

        assertTrue(called.get())

        assertEquals("SELECT B.company_id, B.key, B.name, B.tag_line, B.t_created, B.t_updated FROM brands AS B WHERE (B.key, B.company_id) IN (SELECT I.brand_key, I.company_id FROM items AS I WHERE I.name IN (?, ?))", theSql)

        assertEquals(2, theParams.size)

        val strings = arrayOf(theParams[0] as String, theParams[1] as String)
        Arrays.sort(strings)
        assertEquals("item1", strings[0])
        assertEquals("item2", strings[1])
    }
}