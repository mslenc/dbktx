package com.github.mslenc.dbktx.expr

import com.github.mslenc.asyncdb.DbResultSet
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.conn.query
import com.github.mslenc.dbktx.crud.filter
import com.github.mslenc.dbktx.schemas.test1.Company.Companion.CONTACT_INFO_REF
import com.github.mslenc.dbktx.schemas.test1.ContactInfo
import com.github.mslenc.dbktx.schemas.test1.TestSchema1
import com.github.mslenc.dbktx.schemas.test3.Employee
import com.github.mslenc.dbktx.schemas.test3.TestSchema3
import com.github.mslenc.dbktx.util.smap
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
        TestSchema3.numberOfTables
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

        val deferred = async {
            TestSchema1.COMPANY.query(db) {
                CONTACT_INFO_REF.has {
                    ContactInfo.ADDRESS gte "qwe"
                }
            }
        }

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

        val deferred = async {
            TestSchema1.COMPANY.query(db) {
                CONTACT_INFO_REF.isNotNull()
            }
        }

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
            val query = newEntityQuery(TestSchema1.COMPANY)
            query.filter {
                CONTACT_INFO_REF.has {
                    ContactInfo.ADDRESS gte "qwe"
                }
            }
            query.orderBy(CONTACT_INFO_REF, ContactInfo.ADDRESS)
            query.execute()
        } }

        assertFalse(called.get())

        deferred.await()

        assertTrue(called.get())

        assertEquals("SELECT C.\"id\", C.\"name\", C.\"t_created\", C.\"t_updated\" FROM \"companies\" AS C INNER JOIN \"company_details\" AS CD ON C.\"id\" = CD.\"company_id\" WHERE (CD.\"address\" >= ?) ORDER BY CD.\"address\"", theSql)

        assertEquals(1, theParams.size)
        assertEquals("qwe", theParams[0])

    }

    @Test
    fun testCachedNullsDontCauseTrouble() = runBlocking {
        val connection = object : MockDbConnection() {
            override fun executeQuery(sql: String, values: MutableList<Any?>): CompletableFuture<DbResultSet> {
                if (sql == "SELECT E.\"id\", E.\"first_name\", E.\"last_name\" FROM \"employee\" AS E WHERE E.\"id\" IN (1, 2)") {
                    return CompletableFuture.completedFuture(MockResultSet.Builder().
                            addColumns("id", "first_name", "last_name").
                            addRow(1L, "Johnny", "Smith").
                            addRow(2L, "Mary", "Johnson").
                            build())

                }

                if (sql == "SELECT CI.\"id\", CI.\"first_name\", CI.\"last_name\", CI.\"street_1\", CI.\"street_2\", CI.\"employee_id\" FROM \"contact_info\" AS CI WHERE (TRUE IS NOT DISTINCT FROM (CI.\"employee_id\" IN (1, 2)))") {
                    return CompletableFuture.completedFuture(MockResultSet.Builder().
                            addColumns("id", "first_name", "last_name", "street_1", "street_2", "employee_id").
                            addRow(100L, "John", "Smith", null, null, 1L).
                            build())
                }
                println(sql)

                return CompletableFuture.completedFuture(MockResultSet.Builder().build())
            }
        }

        val db = DbLoaderImpl(connection, this, RequestTime.forTesting())

        val people = db.loadByIds(Employee, listOf(1L, 2L))
        val john = people.getValue(1L)
        val mary = people.getValue(2L)

        listOf(john, mary).smap { it.contactInfo() }

        assertEquals("John Smith", "${john.contactFirstName()} ${john.contactLastName()}")
        assertEquals("null null", "${mary.contactFirstName()} ${mary.contactLastName()}")
    }
}