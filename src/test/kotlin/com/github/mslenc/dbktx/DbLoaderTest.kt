package com.github.mslenc.dbktx

import com.github.mslenc.asyncdb.DbExecResult
import com.github.mslenc.asyncdb.DbResultSet
import com.github.mslenc.asyncdb.impl.DbQueryResultImpl
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.crud.filter
import com.github.mslenc.dbktx.schemas.initSchemas
import com.github.mslenc.dbktx.schemas.test1.*
import com.github.mslenc.dbktx.schemas.test1.TestSchema1.ITEM
import com.github.mslenc.dbktx.util.BatchingLoader
import com.github.mslenc.dbktx.util.FakeRowData
import com.github.mslenc.dbktx.util.makeDbContext
import com.github.mslenc.dbktx.util.testing.MockDbConnection
import com.github.mslenc.dbktx.util.testing.MockResultSet
import com.github.mslenc.dbktx.util.testing.toLDT
import com.github.mslenc.utils.smap
import kotlinx.coroutines.*
import org.junit.Test

import org.junit.Assert.*
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.CompletableFuture

class DbLoaderTest {
    init {
        initSchemas()
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
    fun testSelectForUpdate() = runBlocking {
        var called = false

        val companyId = UUID.randomUUID()

        var theSql = ""
        var theParams: List<Any?> = arrayListOf()

        val conn = object: MockDbConnection() {
            override fun execute(sql: String, args: List<Any?>): CompletableFuture<DbExecResult> {
                called = true

                theSql = sql
                theParams = args

                val result = MockResultSet.Builder("id", "name", "t_created", "t_updated")

                result.addRow(companyId.toString(), "The Company", "2017-06-27T12:44:21".toLDT(), "2017-06-27T12:44:21".toLDT())

                return CompletableFuture.completedFuture(DbQueryResultImpl(0, null, result.build(), null))
            }
        }

        val loader = DbLoaderImpl(conn, this, RequestTime.forTesting())

        val query = loader.newEntityQuery(Company, selectForUpdate = true)
        query.filter { Company.ID eq companyId }

        val result = query.execute()

        assertTrue(called)

        assertEquals("SELECT C.\"id\", C.\"name\", C.\"t_created\", C.\"t_updated\" " +
                "FROM \"companies\" AS C WHERE C.\"id\" = ? FOR UPDATE", theSql)

        checkParams(theParams, companyId.toString())

        assertEquals(1, result.size)
        assertEquals(companyId, result[0].id)
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

                return CompletableFuture.completedFuture(DbQueryResultImpl(0, null, result.build(), null))
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

        assertEquals("SELECT I.\"company_id\", I.\"sku\", I.\"brand_key\", I.\"name\", I.\"price\", I.\"t_created\", I.\"t_updated\" " +
                     "FROM \"items\" AS I WHERE (I.\"company_id\", I.\"sku\") IN ((?, ?), (?, ?), (?, ?), (?, ?))", theSql)

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

                return CompletableFuture.completedFuture(DbQueryResultImpl(0, null, result.build(), null))
            }
        }

        val db = DbLoaderImpl(conn, this, RequestTime.forTesting())


        val com0 = Company(comId0, FakeRowData.of(Company, comId0, "company", "2017-06-01T19:51:22".toLDT(), "2017-06-13T04:23:50".toLDT()))
        val com1 = Company(comId1, FakeRowData.of(Company, comId1, "corporation", "2017-06-02T12:23:34".toLDT(), "2017-06-12T19:30:01".toLDT()))
        val com2 = Company(comId2, FakeRowData.of(Company, comId2, "organization", "2017-06-03T00:00:01".toLDT(), "2017-06-11T22:12:21".toLDT()))

        val futures = arrayOf (
            async { db.load(Company.BRANDS_SET, com2) },
            async { db.load(Company.BRANDS_SET, com0) },
            async { db.load(Company.BRANDS_SET, com1) }
        )

        assertFalse(called)

        val results = arrayOf(
            futures[0].await(),
            futures[1].await(),
            futures[2].await()
        )

        assertTrue(called)

        assertEquals("SELECT B.\"company_id\", B.\"key\", B.\"name\", B.\"tag_line\", B.\"t_created\", B.\"t_updated\" FROM \"brands\" AS B WHERE B.\"company_id\" IN (?, ?, ?)", theSql)
        assertEquals(comId2.toString(), theParams[0] as String)
        assertEquals(comId0.toString(), theParams[1] as String)
        assertEquals(comId1.toString(), theParams[2] as String)

        assertEquals("C.\"id\", C.\"name\", C.\"t_created\", C.\"t_updated\"", TestSchema1.COMPANY.defaultColumnNames)

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

                return CompletableFuture.completedFuture(DbQueryResultImpl(0, null, result.build(), null))
            }
        }

        val db: DbConn = DbLoaderImpl(conn, this, RequestTime.forTesting())

        assertEquals("B.\"company_id\", B.\"key\", B.\"name\", B.\"tag_line\", B.\"t_created\", B.\"t_updated\"", TestSchema1.BRAND.defaultColumnNames)

        val brand0 = Brand(id0, FakeRowData.of(Brand, id0.companyId, id0.key, "Abc (tm)", "We a-b-c for you!", "2017-04-27T12:21:13".toLDT(), "2017-05-27T01:02:03".toLDT()))
        val brand1 = Brand(id1, FakeRowData.of(Brand, id1.companyId, id1.key, "Sheeps Inc.", "Wool and stool!", "2017-02-25T13:31:14".toLDT(), "2017-03-27T02:03:04".toLDT()))
        val brand2 = Brand(id2, FakeRowData.of(Brand, id2.companyId, id2.key, "Gooey Phooey", "Tee hee mee bee", "2017-03-26T14:41:15".toLDT(), "2017-04-27T03:04:05".toLDT()))

        // these should contain the order of the following async call
        val idxof0 = 2
        val idxof1 = 0
        val idxof2 = 1
        val futures: Array<Deferred<List<Item>>> = arrayOf(
            async { db.load(Brand.ITEMS_SET, brand1) },
            async { db.load(Brand.ITEMS_SET, brand2) },
            async { db.load(Brand.ITEMS_SET, brand0) }
        )

        assertFalse(called)

        val results = arrayOf(
            futures[0].await(),
            futures[1].await(),
            futures[2].await()
        )

        assertTrue(called)

        assertEquals("SELECT I.\"company_id\", I.\"sku\", I.\"brand_key\", I.\"name\", I.\"price\", I.\"t_created\", I.\"t_updated\" FROM \"items\" AS I WHERE (I.\"brand_key\", I.\"company_id\") IN ((?, ?), (?, ?), (?, ?))", theSql!!)

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

    @Test
    fun testCustomBatchedLoading() = runBlocking {
        var called = false

        val batch = object : BatchingLoader<Int, String> {
            override suspend fun loadNow(keys: Set<Int>, db: DbConn): Map<Int, String> {
                called = true

                val sorted = TreeSet(keys)
                val result = sorted.joinToString(", ")
                val resultKeys = sorted - sorted.first()
                return resultKeys.associateWith { result }
            }

            override fun nullResult(): String {
                return "null result"
            }
        }

        val conn = MockDbConnection()

        val db: DbConn = DbLoaderImpl(conn, this, RequestTime.forTesting())

        val futures: Array<Deferred<String>> = arrayOf(
            async { db.load(batch, 12) },
            async { db.load(batch, 65) },
            async { db.load(batch, 7) }
        )

        assertFalse(called)

        val results = arrayOf(
            futures[0].await(),
            futures[1].await(),
            futures[2].await()
        )

        assertTrue(called)

        assertEquals("7, 12, 65", results[0]) // result for 12
        assertEquals("7, 12, 65", results[1]) // result for 65
        assertEquals("null result", results[2]) // result for 7


        val result = db.loadForAll(batch, listOf(300, 100, 200, 300))
        assertEquals(3, result.size)
        assertEquals("null result", result[100])
        assertEquals("100, 200, 300", result[200])
        assertEquals("100, 200, 300", result[300])
    }

    @Test
    fun testCustomBatchLoaderInteractionWithRegularBatchLoading() = runBlocking {
        var called = false

        val conn = object : MockDbConnection() {
            override fun execute(sql: String, args: List<Any?>): CompletableFuture<DbExecResult> {
                return when {
                    sql == "SELECT P.\"id\", P.\"company_id\", P.\"t_created\", P.\"t_updated\" FROM \"purchases\" AS P WHERE P.\"id\" IN (7, 12)" && args.isEmpty() -> {
                        val result =
                            MockResultSet.Builder("id", "company_id", "t_created", "t_updated").
                                addRow(7L, UUID.randomUUID(), LocalDateTime.now(), LocalDateTime.now()).
                                addRow(12L, UUID.randomUUID(), LocalDateTime.now(), LocalDateTime.now()).
                                build()

                        CompletableFuture.completedFuture(result.toExecResult())
                    }

                    sql == "SELECT PI.\"id\", PI.\"company_id\", PI.\"sku\", PI.\"purchase_id\", PI.\"price\", PI.\"t_created\", PI.\"t_updated\" FROM \"purchase_items\" AS PI WHERE PI.\"purchase_id\" IN (7)" && args.isEmpty() -> {
                        val result =
                            MockResultSet.Builder("id", "company_id", "sku", "purchase_id", "price", "t_created", "t_updated").
                                addRow(213L, UUID.randomUUID(), "SKU001", 7L, 125.toBigDecimal(), LocalDateTime.now(), LocalDateTime.now()).
                                addRow(215L, UUID.randomUUID(), "SKU301", 7L, 333.toBigDecimal(), LocalDateTime.now(), LocalDateTime.now()).
                                build()

                        CompletableFuture.completedFuture(result.toExecResult())
                    }

                    sql == "SELECT PI.\"id\", PI.\"company_id\", PI.\"sku\", PI.\"purchase_id\", PI.\"price\", PI.\"t_created\", PI.\"t_updated\" FROM \"purchase_items\" AS PI WHERE PI.\"purchase_id\" IN (12)" && args.isEmpty() -> {
                        val result =
                            MockResultSet.Builder("id", "company_id", "sku", "purchase_id", "price", "t_created", "t_updated").
                                addRow(313L, UUID.randomUUID(), "SKU021", 12L, 321.toBigDecimal(), LocalDateTime.now(), LocalDateTime.now()).
                                addRow(314L, UUID.randomUUID(), "SKU321", 12L, 432.toBigDecimal(), LocalDateTime.now(), LocalDateTime.now()).
                                addRow(315L, UUID.randomUUID(), "SKU521", 12L, 543.toBigDecimal(), LocalDateTime.now(), LocalDateTime.now()).
                                build()

                        CompletableFuture.completedFuture(result.toExecResult())
                    }

                    else -> TODO(sql + args)
                }
            }
        }

        val db: DbConn = DbLoaderImpl(conn, this, RequestTime.forTesting())

        val batch = object : BatchingLoader<Long, String> {
            override suspend fun loadNow(keys: Set<Long>, db: DbConn): Map<Long, String> {
                called = true

                val sorted = keys.filter { it != 65L }.sorted()

                // so these should all load at once
                val purchases = sorted.smap { db.loadById(Purchase, it) }

                // and these should load one by one
                val purchaseSizes = purchases.map { it.items().count() }

                return sorted.indices.associate { i ->
                    purchases[i].id to "${ purchases[i].id }(${ purchaseSizes[i] })"
                }
            }

            override fun nullResult(): String {
                return "null result"
            }
        }

        val futures: Array<Deferred<String>> = arrayOf(
            async { db.load(batch, 12) },
            async { db.load(batch, 65) },
            async { db.load(batch, 7) }
        )

        assertFalse(called)

        val results = arrayOf(
            futures[0].await(),
            futures[1].await(),
            futures[2].await()
        )

        assertTrue(called)

        assertEquals("12(3)", results[0]) // result for 12
        assertEquals("null result", results[1]) // result for 65
        assertEquals("7(2)", results[2]) // result for 7
    }

    @Test(expected = IllegalStateException::class)
    fun testCustomBatchLoaderInteractionWithItself() = runBlocking {
        // so this one should fail, because in the first batch, "fg" is being evaluated; at the same time (before "fg" finishes),
        // "defg" is also evaluated, leading to "efg", then to "fg" again; using the normal batching procedure, the second
        // attempt to evaluate "fg" would just put the whole process on the waiting list, never to be finished

        var called = false

        val conn = MockDbConnection()

        val db: DbConn = DbLoaderImpl(conn, this, RequestTime.forTesting())
        val calls = ArrayList<String>()

        val batch = object : BatchingLoader<String, String> {
            override suspend fun loadNow(keys: Set<String>, db: DbConn): Map<String, String> {
                called = true
                calls += TreeSet(keys).toString()

                val bibi = keys.filter { it != "ab" }
                println(bibi)
                val kids = bibi.smap {
                    when {
                        it.length < 2 -> ":)"
                        else -> db.load(this, it.substring(1))
                    }
                }

                return bibi.indices.associate { i ->
                    bibi[i] to bibi[i] + "->" + kids[i]
                }
            }

            override fun nullResult(): String {
                return "null result"
            }
        }

        val futures: Array<Deferred<String>> = arrayOf(
            async { db.load(batch, "abc") },
            async { db.load(batch, "defg") },
            async { db.load(batch, "fg") }
        )

        assertFalse(called)

        futures[0].await()
        futures[1].await()
        futures[2].await()

        assertTrue(called)
    }

    @Test
    fun testCustomBatchLoaderInteractionWithItself2() = runBlocking {
        // unlike the previous case, there is no overlap in evaluations here; so it should be fine

        var called = false

        val conn = MockDbConnection()

        val db: DbConn = DbLoaderImpl(conn, this, RequestTime.forTesting())
        val calls = ArrayList<String>()

        val batch = object : BatchingLoader<String, String> {
            override suspend fun loadNow(keys: Set<String>, db: DbConn): Map<String, String> {
                called = true
                calls += TreeSet(keys).toString()

                val bibi = keys.filter { it != "ab" }
                val kids = bibi.smap {
                    when {
                        it.length < 2 -> ":)"
                        else -> db.load(this, it.substring(1))
                    }
                }

                return bibi.indices.associate { i ->
                    bibi[i] to bibi[i] + "->" + kids[i]
                }
            }

            override fun nullResult(): String {
                return "null result"
            }
        }

        val futures: Array<Deferred<String>> = arrayOf(
            async { db.load(batch, "abcd") },
            async { db.load(batch, "efgh") },
            async { db.load(batch, "ijkl") }
        )

        assertFalse(called)

        val results = arrayOf(
            futures[0].await(),
            futures[1].await(),
            futures[2].await()
        )

        assertTrue(called)

        assertEquals("abcd->bcd->cd->d->:)", results[0])
        assertEquals("efgh->fgh->gh->h->:)", results[1])
        assertEquals("ijkl->jkl->kl->l->:)", results[2])

        assertEquals(4, calls.size)
        assertEquals("[abcd, efgh, ijkl]", calls[0])
        assertEquals("[bcd, fgh, jkl]", calls[1])
        assertEquals("[cd, gh, kl]", calls[2])
        assertEquals("[d, h, l]", calls[3])
    }

    @Test
    fun testCustomBatchLoaderInteractionWithItself3() = runBlocking {
        var numCalls = 0

        val conn = object : MockDbConnection() {
            override fun execute(sql: String, args: List<Any?>): CompletableFuture<DbExecResult> {
                numCalls++

                return when {
                    sql == "SELECT P.\"id\", P.\"company_id\", P.\"t_created\", P.\"t_updated\" FROM \"purchases\" AS P WHERE P.\"id\" = 123" && args.isEmpty() -> {
                        val result =
                                MockResultSet.Builder("id", "company_id", "t_created", "t_updated").
                                        addRow(123L, UUID.randomUUID(), LocalDateTime.now(), LocalDateTime.now()).
                                        build()

                        CompletableFuture.completedFuture(result.toExecResult())
                    }

                    sql == "SELECT PI.\"id\", PI.\"company_id\", PI.\"sku\", PI.\"purchase_id\", PI.\"price\", PI.\"t_created\", PI.\"t_updated\" FROM \"purchase_items\" AS PI WHERE PI.\"purchase_id\" IN (123)" && args.isEmpty() -> {
                        val result =
                                MockResultSet.Builder("id", "company_id", "sku", "purchase_id", "price", "t_created", "t_updated").
                                        addRow(213L, UUID.randomUUID(), "SKUZZZ", 123L, 125.toBigDecimal(), LocalDateTime.now(), LocalDateTime.now()).
                                        addRow(215L, UUID.randomUUID(), "SKU301", 123L, 333.toBigDecimal(), LocalDateTime.now(), LocalDateTime.now()).
                                        build()

                        CompletableFuture.completedFuture(result.toExecResult())
                    }

                    else -> TODO(sql + args)
                }
            }
        }

        val db: DbConn = DbLoaderImpl(conn, this, RequestTime.forTesting())

        withContext(makeDbContext(db)) {
            val batch = object : BatchingLoader<Long, String> {
                override suspend fun loadNow(keys: Set<Long>, db: DbConn): Map<Long, String> {
                    val purchases = keys.map {
                        db.loadById(Purchase, it)
                    }
                    val items = purchases.map {
                        it.items()
                    }

                    return purchases.indices.associate { purchases[it].id to items[it].size.toString() }
                }

                override fun nullResult(): String {
                    return "null result"
                }
            }

            val future1 = async {
                val purchase123 = db.loadById(Purchase, 123L)
                purchase123.items().maxByOrNull { it.sku }?.sku
            }

            val future2 = async {
                db.load(batch, 123L)
            }

            assertEquals(0, numCalls)

            val result1 = future1.await()
            val result2 = future2.await()

            assertEquals("SKUZZZ", result1)
            assertEquals("2", result2)
            assertEquals(2, numCalls)
        }
    }

    @Test
    fun testCustomBatchLoaderInteractionWithItself4() = runBlocking {
        var numCalls = 0

        val conn = object : MockDbConnection() {
            override fun execute(sql: String, args: List<Any?>): CompletableFuture<DbExecResult> {
                numCalls++

                return when {
                    sql == "SELECT P.\"id\", P.\"company_id\", P.\"t_created\", P.\"t_updated\" FROM \"purchases\" AS P WHERE P.\"id\" = 123" && args.isEmpty() -> {
                        val result =
                                MockResultSet.Builder("id", "company_id", "t_created", "t_updated").
                                        addRow(123L, UUID.randomUUID(), LocalDateTime.now(), LocalDateTime.now()).
                                        build()

                        CompletableFuture.completedFuture(result.toExecResult())
                    }

                    sql == "SELECT P.\"id\", P.\"company_id\", P.\"t_created\", P.\"t_updated\" FROM \"purchases\" AS P WHERE P.\"id\" = 234" && args.isEmpty() -> {
                        val result =
                                MockResultSet.Builder("id", "company_id", "t_created", "t_updated").
                                        addRow(234L, UUID.randomUUID(), LocalDateTime.now(), LocalDateTime.now()).
                                        build()

                        CompletableFuture.completedFuture(result.toExecResult())
                    }

                    sql == "SELECT PI.\"id\", PI.\"company_id\", PI.\"sku\", PI.\"purchase_id\", PI.\"price\", PI.\"t_created\", PI.\"t_updated\" FROM \"purchase_items\" AS PI WHERE PI.\"purchase_id\" IN (123)" && args.isEmpty() -> {
                        val result =
                                MockResultSet.Builder("id", "company_id", "sku", "purchase_id", "price", "t_created", "t_updated").
                                        addRow(213L, UUID.randomUUID(), "SKUZZZ", 123L, 125.toBigDecimal(), LocalDateTime.now(), LocalDateTime.now()).
                                        addRow(215L, UUID.randomUUID(), "SKU301", 123L, 333.toBigDecimal(), LocalDateTime.now(), LocalDateTime.now()).
                                        build()

                        CompletableFuture.completedFuture(result.toExecResult())
                    }

                    sql == "SELECT PI.\"id\", PI.\"company_id\", PI.\"sku\", PI.\"purchase_id\", PI.\"price\", PI.\"t_created\", PI.\"t_updated\" FROM \"purchase_items\" AS PI WHERE PI.\"purchase_id\" IN (234)" && args.isEmpty() -> {
                        val result =
                                MockResultSet.Builder("id", "company_id", "sku", "purchase_id", "price", "t_created", "t_updated").
                                        addRow(313L, UUID.randomUUID(), "SKUYYY", 234L, 121.toBigDecimal(), LocalDateTime.now(), LocalDateTime.now()).
                                        addRow(314L, UUID.randomUUID(), "SKUBII", 234L, 122.toBigDecimal(), LocalDateTime.now(), LocalDateTime.now()).
                                        addRow(315L, UUID.randomUUID(), "SKU301", 234L, 333.toBigDecimal(), LocalDateTime.now(), LocalDateTime.now()).
                                        addRow(316L, UUID.randomUUID(), "SKU341", 234L, 334.toBigDecimal(), LocalDateTime.now(), LocalDateTime.now()).
                                        build()

                        CompletableFuture.completedFuture(result.toExecResult())
                    }


                    //

                    else -> TODO(sql + args)
                }
            }
        }

        val db: DbConn = DbLoaderImpl(conn, this, RequestTime.forTesting())

        withContext(makeDbContext(db)) {
            val batch = object : BatchingLoader<Long, String> {
                override suspend fun loadNow(keys: Set<Long>, db: DbConn): Map<Long, String> {
                    val purchases = keys.smap {
                        db.loadById(Purchase, it)
                    }
                    val items = purchases.smap {
                        it.items()
                    }

                    return purchases.indices.associate { purchases[it].id to items[it].size.toString() }
                }

                override fun nullResult(): String {
                    return "null result"
                }
            }

            val future1 = async {
                val purchase123 = db.loadById(Purchase, 123L)
                purchase123.items().maxByOrNull { it.sku }?.sku
            }

            val future2 = async {
                db.load(batch, 123L)
            }

            val future3 = async {
                db.load(batch, 234L)
            }

            assertEquals(0, numCalls)

            val result1 = future1.await()
            val result2 = future2.await()
            val result3 = future3.await()

            assertEquals("SKUZZZ", result1)
            assertEquals("2", result2)
            assertEquals("4", result3)
            assertEquals(4, numCalls)
        }
    }
}

private fun DbResultSet.toExecResult(): DbExecResult {
    return DbQueryResultImpl(0L, null, this, null)
}
