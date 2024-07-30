package com.github.mslenc.dbktx.expr

import com.github.mslenc.asyncdb.DbResultSet
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.conn.query
import com.github.mslenc.dbktx.crud.filter
import com.github.mslenc.dbktx.filters.MatchAnything
import com.github.mslenc.dbktx.filters.MatchNothing
import com.github.mslenc.dbktx.schemas.initSchemas
import com.github.mslenc.dbktx.schemas.test1.Company
import com.github.mslenc.dbktx.schemas.test1.Company.Companion.CONTACT_INFO_REF
import com.github.mslenc.dbktx.schemas.test1.ContactInfo
import com.github.mslenc.dbktx.schemas.test1.TestSchema1
import com.github.mslenc.dbktx.schemas.test3.Employee
import com.github.mslenc.dbktx.util.makeDbContext
import com.github.mslenc.dbktx.util.testing.MockDbConnection
import com.github.mslenc.dbktx.util.testing.MockResultSet
import com.github.mslenc.utils.smap
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.Test

import java.util.concurrent.atomic.AtomicBoolean

import org.junit.Assert.*
import java.util.*
import java.util.concurrent.CompletableFuture

class ExprFilterHasAssociatedTest {
    init {
        initSchemas()
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

        assertEquals("SELECT C.\"id\", C.\"name\", C.\"t_created\", C.\"t_updated\" FROM \"companies\" AS C WHERE C.\"id\" IN (SELECT CI.\"company_id\" FROM \"contact_info\" AS CI WHERE CI.\"address\" >= ?)", theSql)

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

        assertEquals("SELECT C.\"id\", C.\"name\", C.\"t_created\", C.\"t_updated\" FROM \"companies\" AS C WHERE C.\"id\" IN (SELECT CI.\"company_id\" FROM \"contact_info\" AS CI)", theSql)

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

        assertEquals("SELECT C.\"id\", C.\"name\", C.\"t_created\", C.\"t_updated\" FROM \"companies\" AS C INNER JOIN \"contact_info\" AS CI ON C.\"id\" = CI.\"company_id\" WHERE (CI.\"address\" >= ?) ORDER BY CI.\"address\"", theSql)

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

                if (sql == "SELECT CI2.\"id\", CI2.\"first_name\", CI2.\"last_name\", CI2.\"street_1\", CI2.\"street_2\", CI2.\"employee_id\" FROM \"contact_info_2\" AS CI2 WHERE CI2.\"employee_id\" IN (1, 2)") {
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

        withContext(makeDbContext(db)) {
            val people = db.loadByIds(Employee, listOf(1L, 2L))
            val john = people.getValue(1L)
            val mary = people.getValue(2L)

            listOf(john, mary).smap { it.contactInfo() }

            assertEquals("John Smith", "${john.contactFirstName()} ${john.contactLastName()}")
            assertEquals("null null", "${mary.contactFirstName()} ${mary.contactLastName()}")
        }
    }

    @Test
    fun testFilterCheckpoints() = runBlocking {
        val db = DbLoaderImpl(MockDbConnection(), this, RequestTime.forTesting())
        val query = db.newEntityQuery(TestSchema1.COMPANY)

        val check1 = query.checkpoint()
        assertFalse(check1.anyFiltersSince())
        query.filter { Company.NAME eq "test" }
        assertTrue(check1.anyFiltersSince())
        val check2 = query.checkpoint()
        assertFalse(check2.anyFiltersSince())
        query.filter { MatchAnything }
        assertTrue(check1.anyFiltersSince())
        assertFalse(check2.anyFiltersSince())
        query.filter { MatchNothing }
        assertTrue(check1.anyFiltersSince())
        assertTrue(check2.anyFiltersSince())
        val check3 = query.checkpoint()
        assertFalse(check3.anyFiltersSince())
        query.filter { Company.ID eq UUID.randomUUID() }
        assertTrue(check1.anyFiltersSince())
        assertTrue(check2.anyFiltersSince())
        assertFalse(check3.anyFiltersSince())
    }
}