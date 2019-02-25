package com.github.mslenc.dbktx

import com.github.mslenc.asyncdb.DbExecResult
import com.github.mslenc.asyncdb.impl.DbQueryResultImpl
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.schemas.test1.Brand
import com.github.mslenc.dbktx.schemas.test1.Company
import com.github.mslenc.dbktx.schemas.test1.Item
import com.github.mslenc.dbktx.schemas.test1.TestSchema1
import com.github.mslenc.dbktx.schemas.test1.TestSchema1.ITEM
import com.github.mslenc.dbktx.util.FakeRowData
import com.github.mslenc.dbktx.util.testing.MockDbConnection
import com.github.mslenc.dbktx.util.testing.MockResultSet
import com.github.mslenc.dbktx.util.testing.toLDT
import kotlinx.coroutines.*
import org.junit.Before
import org.junit.Test

import org.junit.Assert.*
import java.util.*
import java.util.concurrent.CompletableFuture

class DbLoaderTest {
    @Before
    fun loadSchema() {
        assertNotNull(TestSchema1)
    }

    private fun checkParams(array: List<Any?>, vararg expected: Any) {
        val exp = LinkedHashMap<Any, Int>()
        for (e in expected)
            exp.compute(e) { _, b ->
                if (b == null) 1 else 1 + b
            }

        val actual = LinkedHashMap<Any?, Int>()
        for (e in array)
            actual.compute(e) { _, b -> if (b == null) 1 else 1 + b }

        assertEquals(exp, actual)
    }

    @Test
    fun testBatchedLoadingEntities() = runBlocking {
        var called = false

        val companyId1 = UUID.randomUUID()
        val companyId2 = UUID.randomUUID()

        val id0 = Item.Id(sku = "abc", companyId = companyId1)
        val id1 = Item.Id(sku = "def", companyId = companyId1)
        val id2 = Item.Id(sku = "ghi", companyId = companyId2)
        val id3 = Item.Id(sku = "jkl", companyId = companyId2)

        var theSql = ""
        var theParams: List<Any?> = arrayListOf()

        val conn = object: MockDbConnection() {
            override fun execute(sql: String, args: List<Any?>): CompletableFuture<DbExecResult> {
                called = true

                theSql = sql
                theParams = args

                val result = MockResultSet.Builder("company_id", "sku", "brand_key", "name", "price", "t_created", "t_updated")

                result.addRow(companyId1.toString(), "abc", "bk1", "Item 1", "123.45", "2017-06-27T12:44:21".toLDT(), "2017-06-27T12:44:21".toLDT())
                result.addRow(companyId1.toString(), "def", "bk2", "Item 2", "432.45", "2017-06-26T11:59:09".toLDT(), "2017-06-27T12:44:21".toLDT())
                result.addRow(companyId2.toString(), "ghi", "bk3", "Item 3", "500.45", "2017-06-25T10:00:01".toLDT(), "2017-06-27T22:21:09".toLDT())

                return CompletableFuture.completedFuture(DbQueryResultImpl(0, null, result.build(), emptyList()))
            }
        }

        val loader = DbLoaderImpl(conn, this, RequestTime.forTesting())

        val results = arrayOfNulls<Item>(4)


        val deferred = arrayOf(
            async { loader.findById(ITEM, id2) },
            async { loader.findById(ITEM, id0) },
            async { loader.findById(ITEM, id1) },
            async { loader.findById(ITEM, id3) }
        )

        assertFalse(called)

        results[2] = deferred[0].await()
        results[0] = deferred[1].await()
        results[1] = deferred[2].await()
        results[3] = deferred[3].await()

        assertTrue(called)

        assertEquals("SELECT I.company_id, I.sku, I.brand_key, I.name, I.price, I.t_created, I.t_updated " +
                     "FROM items AS I WHERE (I.company_id, I.sku) IN ((?, ?), (?, ?), (?, ?), (?, ?))", theSql)

        checkParams(theParams, id0.company_id.toString(), id0.sku,
                               id1.company_id.toString(), id1.sku,
                               id2.company_id.toString(), id2.sku,
                               id3.company_id.toString(), id3.sku)


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
        var called = false
        var theSql = ""
        var theParams: List<Any?> = emptyList()

        val comId0 = UUID.randomUUID()
        val comId1 = UUID.randomUUID()
        val comId2 = UUID.randomUUID()


        val conn = object: MockDbConnection() {
            override fun execute(sql: String, args: List<Any?>): CompletableFuture<DbExecResult> {
                called = true
                theSql = sql
                theParams = args

                val result = MockResultSet.Builder("company_id", "key", "name", "tag_line", "t_created", "t_updated")
                result.addRow(comId1.toString(), "abc", "Abc (tm)", "We a-b-c for you!", "2017-04-27T14:41:14".toLDT(), "2017-05-27T08:22:12".toLDT())
                result.addRow(comId2.toString(), "baa", "Sheeps Inc.", "Wool and stool!", "2017-02-25T12:21:12".toLDT(), "2017-03-27T09:41:21".toLDT())
                result.addRow(comId1.toString(), "goo", "Gooey Phooey", "Tee hee mee bee", "2017-03-26T16:51:14".toLDT(), "2017-04-27T10:50:00".toLDT())

                return CompletableFuture.completedFuture(DbQueryResultImpl(0, null, result.build(), emptyList()))
            }
        }

        val db = DbLoaderImpl(conn, this, RequestTime.forTesting())


        val com0 = Company(db, comId0, FakeRowData.of(Company, comId0, "company", "2017-06-01T19:51:22".toLDT(), "2017-06-13T04:23:50".toLDT()))
        val com1 = Company(db, comId1, FakeRowData.of(Company, comId1, "corporation", "2017-06-02T12:23:34".toLDT(), "2017-06-12T19:30:01".toLDT()))
        val com2 = Company(db, comId2, FakeRowData.of(Company, comId2, "organization", "2017-06-03T00:00:01".toLDT(), "2017-06-11T22:12:21".toLDT()))

        val futures = arrayOf (
            async { db.load(com2, Company.BRANDS_SET) },
            async { db.load(com0, Company.BRANDS_SET) },
            async { db.load(com1, Company.BRANDS_SET) }
        )

        assertFalse(called)

        val results = arrayOf(
            futures[0].await(),
            futures[1].await(),
            futures[2].await()
        )

        assertTrue(called)

        assertEquals("SELECT B.company_id, B.key, B.name, B.tag_line, B.t_created, B.t_updated FROM brands AS B WHERE B.company_id IN (?, ?, ?)", theSql)
        assertEquals(comId2.toString(), theParams[0] as String)
        assertEquals(comId0.toString(), theParams[1] as String)
        assertEquals(comId1.toString(), theParams[2] as String)

        assertEquals("C.id, C.name, C.t_created, C.t_updated", TestSchema1.COMPANY.defaultColumnNames)

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
        var called = false

        val id0 = Brand.Id("abc", UUID.randomUUID())
        val id1 = Brand.Id("baa", UUID.randomUUID())
        val id2 = Brand.Id("goo", UUID.randomUUID())

        var theSql: String? = null
        var theParams: List<Any?> = emptyList()
        val conn = object : MockDbConnection() {
            override fun execute(sql: String, args: List<Any?>): CompletableFuture<DbExecResult> {
                called = true
                theSql = sql
                theParams = args

                val result = MockResultSet.Builder("company_id", "sku", "brand_key", "name", "price", "t_created", "t_updated")
                result.addRow(id1.companyId.toString(), "SHP001", id1.key, "A white sheep", "412.50", "2017-04-27T20:56:56".toLDT(), "2017-05-27T11:22:33".toLDT())
                result.addRow(id1.companyId.toString(), "SHP010", id1.key, "A black sheep", "999.95", "2017-03-27T21:57:57".toLDT(), "2017-04-27T22:11:00".toLDT())
                result.addRow(id1.companyId.toString(), "TOO001", id1.key, "A fine wool trimmer", "111.11", "2017-04-27T22:58:58".toLDT(), "2017-05-27T00:01:02".toLDT())
                result.addRow(id2.companyId.toString(), "GOO",    id2.key, "The Goo", "4.50", "2016-01-01T23:59:59".toLDT(), "2016-01-01T01:02:03".toLDT())

                return CompletableFuture.completedFuture(DbQueryResultImpl(0, null, result.build(), emptyList()))
            }
        }

        val db: DbConn = DbLoaderImpl(conn, this, RequestTime.forTesting())

        assertEquals("B.company_id, B.key, B.name, B.tag_line, B.t_created, B.t_updated", TestSchema1.BRAND.defaultColumnNames)

        val brand0 = Brand(db, id0, FakeRowData.of(Brand, id0.companyId, id0.key, "Abc (tm)", "We a-b-c for you!", "2017-04-27T12:21:13".toLDT(), "2017-05-27T01:02:03".toLDT()))
        val brand1 = Brand(db, id1, FakeRowData.of(Brand, id1.companyId, id1.key, "Sheeps Inc.", "Wool and stool!", "2017-02-25T13:31:14".toLDT(), "2017-03-27T02:03:04".toLDT()))
        val brand2 = Brand(db, id2, FakeRowData.of(Brand, id2.companyId, id2.key, "Gooey Phooey", "Tee hee mee bee", "2017-03-26T14:41:15".toLDT(), "2017-04-27T03:04:05".toLDT()))

        // these should contain the order of the following async call
        val idxof0 = 2
        val idxof1 = 0
        val idxof2 = 1
        val futures: Array<Deferred<List<Item>>> = arrayOf(
            async { db.load(brand1, Brand.ITEMS_SET) },
            async { db.load(brand2, Brand.ITEMS_SET) },
            async { db.load(brand0, Brand.ITEMS_SET) }
        )

        assertFalse(called)

        val results = arrayOf(
            futures[0].await(),
            futures[1].await(),
            futures[2].await()
        )

        assertTrue(called)

        assertEquals("SELECT I.company_id, I.sku, I.brand_key, I.name, I.price, I.t_created, I.t_updated FROM items AS I WHERE (I.brand_key, I.company_id) IN ((?, ?), (?, ?), (?, ?))", theSql!!)

        checkParams(theParams, id0.key, id0.companyId.toString(),
                               id1.key, id1.companyId.toString(),
                               id2.key, id2.companyId.toString())

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