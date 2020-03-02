package com.github.mslenc.dbktx.expr

import com.github.mslenc.asyncdb.DbResultSet
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.conn.query
import com.github.mslenc.dbktx.runMysqlTest
import com.github.mslenc.dbktx.schemas.test1.*
import com.github.mslenc.dbktx.schemas.test1.Brand.Companion.ITEMS_SET
import com.github.mslenc.dbktx.schemas.test3.Employee
import com.github.mslenc.dbktx.util.smap
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
    init {
        TestSchema1.numberOfTables // init
    }

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

        val deferred = async {
            TestSchema1.BRAND.query(db) {
                ITEMS_SET.contains {
                    Item.NAME oneOf setOf("item1", "item2")
                }
            }
        }

        assertFalse(called.get())

        deferred.await()

        assertTrue(called.get())

        assertEquals("SELECT B.\"company_id\", B.\"key\", B.\"name\", B.\"tag_line\", B.\"t_created\", B.\"t_updated\" FROM \"brands\" AS B WHERE (B.\"key\", B.\"company_id\") IN (SELECT DISTINCT I.\"brand_key\", I.\"company_id\" FROM \"items\" AS I WHERE I.\"name\" IN (?, ?))", theSql)

        assertEquals(2, theParams.size)

        val strings = arrayOf(theParams[0] as String, theParams[1] as String)
        Arrays.sort(strings)
        assertEquals("item1", strings[0])
        assertEquals("item2", strings[1])
    }

    @Test
    fun runSanityCheck1() = runMysqlTest { db ->
        val people = db.loadByIds(Employee, listOf(1L, 2L))
        val john = people.getValue(1L)
        val mary = people.getValue(2L)

        val contacts = listOf(john, mary).smap { it.contactInfo() }

        assertNotNull(contacts[0])
        assertNull(contacts[1])

        assertEquals("Johnny", john.firstName)
        assertEquals("John", john.contactFirstName())
    }

    @Test
    fun runSanityCheck2() = runMysqlTest { db ->
        val result = with (ContactInfo) { query(db) {
            !COMPANY_REF.has {
                Company.NAME gte "qwe"
            }
        } }
    }
}