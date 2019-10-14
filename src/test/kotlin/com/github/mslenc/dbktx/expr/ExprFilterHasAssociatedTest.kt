package com.github.mslenc.dbktx.expr

import com.github.mslenc.asyncdb.DbResultSet
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.crud.filter
import com.github.mslenc.dbktx.schemas.test1.Company.Companion.CONTACT_INFO_REF
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

class ExprFilterHasAssociatedTest {
    init {
        TestSchema1.numberOfTables // init
    }

    @Test
    fun testAssociatedQuery() = runBlocking {
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
            TestSchema1.COMPANY.query {
                CONTACT_INFO_REF.has {
                    ContactInfo.ADDRESS gte "qwe"
                }
            }
        } }

        assertFalse(called.get())

        deferred.await()

        assertTrue(called.get())

        assertEquals("SELECT C.\"id\", C.\"name\", C.\"t_created\", C.\"t_updated\" FROM \"companies\" AS C WHERE C.\"id\" IN (SELECT CD.\"company_id\" FROM \"company_details\" AS CD WHERE CD.\"address\" >= ?)", theSql)

        assertEquals(1, theParams.size)
        assertEquals("qwe", theParams[0])

    }

    @Test
    fun testAssociatedQueryWithNoCondition() = runBlocking {
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
            TestSchema1.COMPANY.query {
                CONTACT_INFO_REF.isNotNull()
            }
        } }

        assertFalse(called.get())

        deferred.await()

        assertTrue(called.get())

        assertEquals("SELECT C.\"id\", C.\"name\", C.\"t_created\", C.\"t_updated\" FROM \"companies\" AS C WHERE C.\"id\" IN (SELECT CD.\"company_id\" FROM \"company_details\" AS CD)", theSql)

        assertEquals(0, theParams.size)
    }

    @Test
    fun testAssociatedQueryInPresenceOfJoins() = runBlocking {
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
            val query = newQuery(TestSchema1.COMPANY)
            query.filter {
                CONTACT_INFO_REF.has {
                    ContactInfo.ADDRESS gte "qwe"
                }
            }
            query.orderBy(CONTACT_INFO_REF, ContactInfo.ADDRESS)
            query.run()
        } }

        assertFalse(called.get())

        deferred.await()

        assertTrue(called.get())

        assertEquals("SELECT C.\"id\", C.\"name\", C.\"t_created\", C.\"t_updated\" FROM \"companies\" AS C INNER JOIN \"company_details\" AS CD ON C.\"id\" = CD.\"company_id\" WHERE (CD.\"address\" >= ?) ORDER BY CD.\"address\"", theSql)

        assertEquals(1, theParams.size)
        assertEquals("qwe", theParams[0])

    }
}