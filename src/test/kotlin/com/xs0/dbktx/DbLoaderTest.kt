package com.xs0.dbktx

import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.conn.DbLoaderImpl
import com.xs0.dbktx.schemas.test1.Brand
import com.xs0.dbktx.schemas.test1.Company
import com.xs0.dbktx.schemas.test1.Item
import com.xs0.dbktx.schemas.test1.TestSchema1
import com.xs0.dbktx.schemas.test1.TestSchema1.ITEM
//import com.xs0.dbktx.schemas.test1.TestSchema1.tablesByDbName
import com.xs0.dbktx.util.DelayedExec
import com.xs0.dbktx.util.MockSQLConnection
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.SQLConnection
import kotlinx.coroutines.experimental.Deferred
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Before
import org.junit.Test

import java.util.LinkedHashMap

import java.util.Arrays.asList
import org.junit.Assert.*

class DbLoaderTest {
    @Before
    fun loadSchema() {
        assertNotNull(TestSchema1)
    }

    private fun checkParams(array: JsonArray, vararg expected: Any) {
        val exp = LinkedHashMap<Any, Int>()
        for (e in expected)
            exp.compute(e) { a, b ->
                if (b == null) 1 else 1 + b
            }

        val actual = LinkedHashMap<Any, Int>()
        for (e in array.list)
            actual.compute(e!!) { a, b -> if (b == null) 1 else 1 + b }

        assertEquals(exp, actual)
    }

    fun array(vararg elements: Any): JsonArray {
        return JsonArray(elements.toMutableList())
    }

    @Test
    fun testBatchedLoadingEntities() = runBlocking {
        var called = false

        val id0 = Item.Id(sku = "abc", companyId = 123L)
        val id1 = Item.Id(sku = "def", companyId = 123L)
        val id2 = Item.Id(sku = "ghi", companyId = 234L)
        val id3 = Item.Id(sku = "jkl", companyId = 234L)

        var theSql = ""
        var theParams = JsonArray()

        val conn = object: MockSQLConnection() {
            override fun queryWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
                called = true

                theSql = sql
                theParams = params

                val result = ResultSet(
                        asList("company_id", "sku", "brand_key", "name", "price", "t_created", "t_updated"),
                        asList(
                                array(123L, "abc", "bk1", "Item 1", "123.45", "2017-06-27", "2017-06-27"),
                                array(123L, "def", "bk2", "Item 2", "432.45", "2017-06-26", "2017-06-27"),
                                array(234L, "ghi", "bk3", "Item 3", "500.45", "2017-06-25", "2017-06-27")
                        ), null
                )

                resultHandler.handle(Future.succeededFuture(result))

                return this
            }
        }

        val delayedExec = DelayedExec()
        val loader = DbLoaderImpl(conn, delayedExec)

        val results = arrayOfNulls<Item>(4)


        val deferred = arrayOf(
            loader.findAsync(ITEM, id2),
            loader.findAsync(ITEM, id0),
            loader.findAsync(ITEM, id1),
            loader.findAsync(ITEM, id3)
        )

        assertFalse(called)
        delayedExec.executePending()
        assertTrue(called)

        assertEquals("SELECT company_id, sku, brand_key, name, price, t_created, t_updated " +
                     "FROM items WHERE (company_id, sku) IN ((234, ?), (123, ?), (123, ?), (234, ?))", theSql)

        checkParams(theParams, id0.sku, id1.sku, id2.sku, id3.sku)

        results[2] = deferred[0].await()
        results[0] = deferred[1].await()
        results[1] = deferred[2].await()
        results[3] = deferred[3].await()

        assertNotNull(results[0])
        assertNotNull(results[1])
        assertNotNull(results[2])
        assertNull(results[3])

        assertEquals(id0, results[0]!!.id)
        assertEquals(id1, results[1]!!.id)
        assertEquals(id2, results[2]!!.id)

    }

    @Test
    fun testBatchedLoadingToMany() = runBlocking {
        val delayedExec = DelayedExec()
        var theSql = ""
        var called = false
        val comId0 = 412L
        val comId1 = 314L
        val comId2 = 541515L


        val conn = object: MockSQLConnection() {
            override fun query(sql: String, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
                called = true
                theSql = sql

                resultHandler.handle(Future.succeededFuture(ResultSet(
                        asList("company_id", "key", "name", "tag_line", "t_created", "t_updated"),
                        asList(
                                array(comId1, "abc", "Abc (tm)", "We a-b-c for you!", "2017-04-27", "2017-05-27"),
                                array(comId2, "baa", "Sheeps Inc.", "Wool and stool!", "2017-02-25", "2017-03-27"),
                                array(comId1, "goo", "Gooey Phooey", "Tee hee mee bee", "2017-03-26", "2017-04-27")
                        ), null
                )))
                return this
            }
        }

        val db = DbLoaderImpl(conn, delayedExec)


        val com0 = Company(db, comId0, listOf<Any?>(comId0, "company", "2017-06-01", "2017-06-13"))
        val com1 = Company(db, comId1, listOf<Any?>(comId1, "corporation", "2017-06-02", "2017-06-12"))
        val com2 = Company(db, comId2, listOf<Any?>(comId2, "organization", "2017-06-03", "2017-06-11"))

        val futures = arrayOf (
            db.loadAsync(com2, Company.BRANDS_SET),
            db.loadAsync(com0, Company.BRANDS_SET),
            db.loadAsync(com1, Company.BRANDS_SET)
        )

        assertFalse(called)
        delayedExec.executePending()
        assertTrue(called)

        assertEquals("SELECT B.company_id, B.key, B.name, B.tag_line, B.t_created, B.t_updated FROM brands AS B WHERE company_id IN ($comId2, $comId0, $comId1)", theSql)

        assertEquals("C.id, C.name, C.t_created, C.t_updated", TestSchema1.COMPANY.defaultColumnNames)

        val results = arrayOf(
            futures[0].await(),
            futures[1].await(),
            futures[2].await()
        )

        assertNotNull(results[0])
        assertNotNull(results[1])
        assertNotNull(results[2])

        assertEquals(1, results[0].size)
        assertEquals(0, results[1].size)
        assertEquals(2, results[2].size)

//         TODO: check some props as well
    }

    @Test
    fun testBatchedLoadingToManyMultiField() = runBlocking {
        val delayedExec = DelayedExec()

        var called = false

        val id0 = Brand.Id("abc", 1024L)
        val id1 = Brand.Id("baa", 256L)
        val id2 = Brand.Id("goo", 16L)

        var theSql: String? = null
        var theParams: JsonArray? = null
        var conn = object : MockSQLConnection() {
            override fun queryWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
                called = true
                theSql = sql
                theParams = params

                val resultSet = ResultSet(
                        asList("company_id", "key", "name", "tag_line", "t_created", "t_updated"),
                        asList(
                                array(id1.companyId, "SHP001", id1.key, "A white sheep", "412.50", "2017-04-27", "2017-05-27"),
                                array(id1.companyId, "SHP010", id1.key, "A black sheep", "999.95", "2017-03-27", "2017-04-27"),
                                array(id1.companyId, "TOO001", id1.key, "A fine wool trimmer", "111.11", "2017-04-27", "2017-05-27"),
                                array(id2.companyId, "GOO",    id2.key, "The Goo", "4.50", "2016-01-01", "2016-01-01")
                        ), null
                )

                resultHandler.handle(Future.succeededFuture(resultSet))
                return this
            }
        }

        val db: DbConn = DbLoaderImpl(conn, delayedExec)

        assertEquals("B.company_id, B.key, B.name, B.tag_line, B.t_created, B.t_updated", TestSchema1.BRAND.defaultColumnNames)

        val brand0 = Brand(db, id0, asList(id0.companyId, id0.key, "Abc (tm)", "We a-b-c for you!", "2017-04-27", "2017-05-27"))
        val brand1 = Brand(db, id1, asList(id1.companyId, id1.key, "Sheeps Inc.", "Wool and stool!", "2017-02-25", "2017-03-27"))
        val brand2 = Brand(db, id2, asList(id2.companyId, id2.key, "Gooey Phooey", "Tee hee mee bee", "2017-03-26", "2017-04-27"))

        // these should contain the order of the following async call
        val idxof0 = 2
        val idxof1 = 0
        val idxof2 = 1
        val futures: Array<Deferred<List<Item>>> = arrayOf(
            db.loadAsync(brand1, Brand.ITEMS_SET),
            db.loadAsync(brand2, Brand.ITEMS_SET),
            db.loadAsync(brand0, Brand.ITEMS_SET)
        )

        assertFalse(called)
        delayedExec.executePending()
        assertTrue(called)

        assertEquals("SELECT company_id, sku, brand_key, name, price, t_created, t_updated FROM items WHERE (brand_key, company_id) IN ((?, 256), (?, 16), (?, 1024))", theSql!!)

        checkParams(theParams!!, id0.key, id1.key, id2.key)

        val results = arrayOf(
            futures[0].await(),
            futures[1].await(),
            futures[2].await()
        )

        assertNotNull(results[0])
        assertNotNull(results[1])
        assertNotNull(results[2])

//        assertTrue(results[0]!!.succeeded())
//        assertTrue(results[1]!!.succeeded())
//        assertTrue(results[2]!!.succeeded())

        assertEquals(0, results[idxof0].size)
        assertEquals(3, results[idxof1].size)
        assertEquals(1, results[idxof2].size)

//         TODO: check some props as well
    }
}