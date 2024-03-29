package com.github.mslenc.dbktx.util

import com.github.mslenc.asyncdb.DbExecResult
import com.github.mslenc.asyncdb.impl.DbQueryResultImpl
import com.github.mslenc.asyncdb.util.EmptyResultSet
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.RequestTime
import com.github.mslenc.dbktx.crud.filter
import com.github.mslenc.dbktx.schemas.initSchemas
import com.github.mslenc.dbktx.schemas.test1.Brand
import com.github.mslenc.dbktx.schemas.test1.Company
import com.github.mslenc.dbktx.util.testing.MockDbConnection
import kotlinx.coroutines.runBlocking
import org.junit.Assert
import org.junit.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean

class TextSearchBuilderTest {
    init {
        initSchemas()
    }

    @Test
    fun testRemapPreservesCaseInsensitivity() = runBlocking {
        val called = AtomicBoolean(false)
        var theSql: String? = null
        var theParams: List<Any?> = emptyList()

        val connection = object : MockDbConnection() {
            override fun execute(sql: String, args: List<Any?>): CompletableFuture<DbExecResult> {
                called.set(true)
                theSql = sql
                theParams = args

                val result = DbQueryResultImpl(0L, null, EmptyResultSet.INSTANCE, EmptyResultSet.INSTANCE)
                return CompletableFuture.completedFuture(result)
            }
        }

        val now = RequestTime.forTesting()
        val db = DbLoaderImpl(connection, this, now)

        val brands = db.newEntityQuery(Brand)
        TextSearchBuilder(brands, "Hello World").
            matchAnywhereIn(Brand.NAME).
            applyToQuery()

        val otherQuery = db.newEntityQuery(Company)
        otherQuery.filter { Company.BRANDS_SET contains brands }
        otherQuery.execute()


        Assert.assertTrue(called.get())

        Assert.assertEquals("SELECT C.\"id\", C.\"name\", C.\"t_created\", C.\"t_updated\" FROM \"companies\" AS C WHERE C.\"id\" IN (SELECT DISTINCT B.\"company_id\" FROM \"brands\" AS B WHERE (B.\"name\" ILIKE ? ESCAPE '|') AND (B.\"name\" ILIKE ? ESCAPE '|'))", theSql)

        Assert.assertEquals(2, theParams.size)
        Assert.assertEquals("%Hello%", theParams[0])
        Assert.assertEquals("%World%", theParams[1])
    }
}