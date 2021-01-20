package com.github.mslenc.dbktx.updates

import com.github.mslenc.asyncdb.DbResultSet
import com.github.mslenc.asyncdb.DbUpdateResult
import com.github.mslenc.asyncdb.impl.DbQueryResultImpl
import com.github.mslenc.asyncdb.util.FutureUtils.failedFuture
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.conn.update
import com.github.mslenc.dbktx.conn.updateOneByOne
import com.github.mslenc.dbktx.crud.dsl.concatWs
import com.github.mslenc.dbktx.schemas.initSchemas
import com.github.mslenc.dbktx.schemas.test1.Item
import com.github.mslenc.dbktx.schemas.test3.ServiceLine
import com.github.mslenc.dbktx.util.testing.MockDbConnection
import com.github.mslenc.dbktx.util.testing.MockResultSet
import com.github.mslenc.dbktx.util.testing.toLDT
import kotlinx.coroutines.runBlocking
import org.junit.Test

import java.util.concurrent.atomic.AtomicBoolean

import org.junit.Assert.*
import java.math.BigDecimal
import java.util.*
import java.util.concurrent.CompletableFuture

class UpdateTest {
    init {
        initSchemas()
    }

    @Test
    fun testUpdate1() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val companyId = UUID.randomUUID()

        val connection = object : MockDbConnection() {
            override fun executeUpdate(sql: String, values: List<Any>): CompletableFuture<DbUpdateResult> {
                called.set(true)
                theSql = sql
                theParams = values
                val result = DbQueryResultImpl(1L, null, null, null)
                return CompletableFuture.completedFuture(result)
            }

            override fun executeQuery(sql: String, values: MutableList<Any?>): CompletableFuture<DbResultSet> {
                try {
                    assertEquals("SELECT I.\"company_id\", I.\"sku\", I.\"brand_key\", I.\"name\", I.\"price\", I.\"t_created\", I.\"t_updated\" FROM \"items\" AS I WHERE (I.\"company_id\", I.\"sku\") = (?, ?)", sql)
                    assertEquals(listOf(companyId.toString(), "LOG0001"), values)
                } catch (e: Throwable) {
                    return failedFuture(e)
                }

                val result = MockResultSet.Builder("company_id", "sku", "brand_key", "name", "price", "t_created", "t_updated")

                result.addRow(companyId.toString(), "LOG0001", "bk1", "Item 1", "123.45", "2017-06-27T12:44:21".toLDT(), "2017-06-27T12:44:21".toLDT())

                return CompletableFuture.completedFuture(result.build())
            }
        }

        val db = DbLoaderImpl(connection, this, RequestTime.forTesting())

        val item = db.loadById(Item, Item.Id(companyId, "LOG0001"))

        val updated = Item.update(item, db) {
            it[NAME] = "abc"
            it[PRICE] += 2.3.toBigDecimal()
            it[PRICE] becomes { PRICE + BigDecimal("12.0") }
            it[PRICE] becomes { PRICE + PRICE / 2.toBigDecimal() }
        }

        assertTrue(called.get())

        assertEquals("UPDATE \"items\" SET \"name\"=?, \"price\"=\"price\" + (\"price\" / 2) WHERE (\"company_id\", \"sku\") = (?, ?)", theSql)

        assertEquals(3, theParams.size)
        assertEquals("abc", theParams[0])
        assertEquals(companyId.toString(), theParams[1])
        assertEquals("LOG0001", theParams[2])
        assertEquals(true, updated)
    }

    @Test
    fun testBatchedUpdate1() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val connection = object : MockDbConnection() {
            override fun executeUpdate(sql: String, values: List<Any>): CompletableFuture<DbUpdateResult> {
                called.set(true)
                theSql = sql
                theParams = values
                val result = DbQueryResultImpl(1L, null, null, null)
                return CompletableFuture.completedFuture(result)
            }

            override fun executeQuery(sql: String, values: MutableList<Any?>): CompletableFuture<DbResultSet> {
                val id = sql.substring(sql.length - 2)

                try {
                    assertEquals("SELECT SL.\"id\", SL.\"name\", SL.\"sort_index\", SL.\"parent_id\" FROM \"service_line\" AS SL WHERE SL.\"id\" = $id", sql)
                    assertTrue(values.isEmpty())
                } catch (e: Throwable) {
                    return failedFuture(e)
                }

                val result = MockResultSet.Builder("id", "name", "sort_index", "parent_id")

                result.addRow(id.toLong(), "SL $id", id.toInt() - 10, null)

                return CompletableFuture.completedFuture(result.build())
            }
        }

        val db = DbLoaderImpl(connection, this, RequestTime.forTesting())

        val item1 = db.loadById(ServiceLine, 11L)
        val item2 = db.loadById(ServiceLine, 12L)
        val item3 = db.loadById(ServiceLine, 13L)

        ServiceLine.updateOneByOne(listOf(item1, item2, item3, item3), db) { update, _, index ->
            when (index) {
                0 -> {
                    update[SORT_INDEX] = 111
                    update[PARENT_ID] = null
                }
                1 -> {
                    update[SORT_INDEX] = 2
                    update[NAME] becomes { concatWs(", ", +NAME, NAME.makeLiteral("Updated")) }
                }
                2 -> {
                    update[SORT_INDEX] = 33
                }
                3 -> {
                    update[PARENT_ID] = 11L
                }
            }
        }

        assertTrue(called.get())

        assertEquals("UPDATE \"service_line\" SET \"sort_index\" = CASE \"id\" WHEN 11 THEN 111 WHEN 13 THEN 33 ELSE \"sort_index\" END, \"name\" = CASE \"id\" WHEN 12 THEN CONCAT_WS(?, \"name\", ?) ELSE \"name\" END, \"parent_id\" = CASE \"id\" WHEN 13 THEN 11 ELSE \"parent_id\" END WHERE \"id\" IN (11, 13, 12)", theSql)

        assertEquals(2, theParams.size)
        assertEquals(", ", theParams[0])
        assertEquals("Updated", theParams[1])
    }
}