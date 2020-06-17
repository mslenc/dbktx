package com.github.mslenc.dbktx.expr

import com.github.mslenc.asyncdb.DbResultSet
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.conn.query
import com.github.mslenc.dbktx.crud.filter
import com.github.mslenc.dbktx.crud.orderMatchesFirst
import com.github.mslenc.dbktx.schemas.initSchemas
import com.github.mslenc.dbktx.schemas.test1.Brand.Companion.COMPANY_REF
import com.github.mslenc.dbktx.schemas.test1.Company
import com.github.mslenc.dbktx.schemas.test1.ContactInfo
import com.github.mslenc.dbktx.schemas.test1.TestSchema1
import com.github.mslenc.dbktx.util.testing.MockDbConnection
import com.github.mslenc.dbktx.util.testing.MockResultSet
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.Test

import java.util.concurrent.atomic.AtomicBoolean

import org.junit.Assert.*
import java.util.concurrent.CompletableFuture

class ExprFilterHasParentTest {
    init {
        initSchemas()
    }

    @Test
    fun testParentQuery() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val connection = object : MockDbConnection() {
            override fun executeQuery(sql: String, values: MutableList<Any?>): CompletableFuture<DbResultSet> {
                called.set(true)
                theSql = sql
                theParams = values

                return CompletableFuture.completedFuture(MockResultSet.Builder().build())
            }
        }

        val db = DbLoaderImpl(connection, this, RequestTime.forTesting())

        val deferred = async {
            TestSchema1.BRAND.query(db) {
                COMPANY_REF.has {
                    Company.NAME gte "qwe"
                }
            }
        }

        assertFalse(called.get())

        deferred.await()

        assertTrue(called.get())

        assertEquals("SELECT B.\"company_id\", B.\"key\", B.\"name\", B.\"tag_line\", B.\"t_created\", B.\"t_updated\" FROM \"brands\" AS B WHERE B.\"company_id\" IN (SELECT C.\"id\" FROM \"companies\" AS C WHERE C.\"name\" >= ?)", theSql)

        assertEquals(1, theParams.size)
        assertEquals("qwe", theParams[0])

    }

    @Test
    fun testParentQueryInPresenceOfJoins() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val connection = object : MockDbConnection() {
            override fun executeQuery(sql: String, values: MutableList<Any?>): CompletableFuture<DbResultSet> {
                called.set(true)
                theSql = sql
                theParams = values

                return CompletableFuture.completedFuture(MockResultSet.Builder().build())
            }
        }

        val db = DbLoaderImpl(connection, this, RequestTime.forTesting())

        val deferred = db.run { async {
            val query = newEntityQuery(TestSchema1.BRAND)
            query.filter {
                COMPANY_REF.has {
                    Company.NAME gte "qwe"
                }
            }
            query.orderBy(COMPANY_REF, Company.NAME)
            query.execute()
        } }

        assertFalse(called.get())

        deferred.await()

        assertTrue(called.get())

        assertEquals("SELECT B.\"company_id\", B.\"key\", B.\"name\", B.\"tag_line\", B.\"t_created\", B.\"t_updated\" FROM \"brands\" AS B INNER JOIN \"companies\" AS C ON C.\"id\" = B.\"company_id\" WHERE (C.\"name\" >= ?) ORDER BY C.\"name\"", theSql)

        assertEquals(1, theParams.size)
        assertEquals("qwe", theParams[0])

    }

    @Test
    fun testParentQueryWhenParentIsNullable() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val connection = object : MockDbConnection() {
            override fun executeQuery(sql: String, values: MutableList<Any?>): CompletableFuture<DbResultSet> {
                called.set(true)
                theSql = sql
                theParams = values

                return CompletableFuture.completedFuture(MockResultSet.Builder().build())
            }
        }

        val db = DbLoaderImpl(connection, this, RequestTime.forTesting())

        val deferred = async { with (ContactInfo) { query(db) {
            COMPANY_REF.has {
                Company.NAME gte "qwe"
            }
        } } }

        assertFalse(called.get())

        deferred.await()

        assertTrue(called.get())

        assertEquals("SELECT CI.\"id\", CI.\"company_id\", CI.\"address\" FROM \"contact_info\" AS CI WHERE (TRUE IS NOT DISTINCT FROM (CI.\"company_id\" IN (SELECT C.\"id\" FROM \"companies\" AS C WHERE C.\"name\" >= ?)))", theSql)

        assertEquals(1, theParams.size)
        assertEquals("qwe", theParams[0])

    }

    @Test
    fun testOrderByMatchesFirst() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val connection = object : MockDbConnection() {
            override fun executeQuery(sql: String, values: MutableList<Any?>): CompletableFuture<DbResultSet> {
                called.set(true)
                theSql = sql
                theParams = values

                return CompletableFuture.completedFuture(MockResultSet.Builder().build())
            }
        }

        val db = DbLoaderImpl(connection, this, RequestTime.forTesting())

        val query = db.newEntityQuery(ContactInfo)
        query.orderMatchesFirst { ContactInfo.ADDRESS.contains("Tokyo") }
        query.orderBy(ContactInfo.COMPANY_REF, Company.NAME)
        query.execute()

        assertTrue(called.get())

        assertEquals("SELECT CI.\"id\", CI.\"company_id\", CI.\"address\" FROM \"contact_info\" AS CI LEFT JOIN \"companies\" AS C ON C.\"id\" = CI.\"company_id\" ORDER BY CASE WHEN CI.\"address\" LIKE ? ESCAPE '|' THEN 0 ELSE 1 END, C.\"name\"", theSql)

        assertEquals(1, theParams.size)
        assertEquals("%Tokyo%", theParams[0])
    }
}