package com.xs0.dbktx.expr

import com.xs0.dbktx.conn.DbLoaderImpl
import com.xs0.dbktx.schemas.test1.TestSchema1
import com.xs0.dbktx.util.DelayedExec
import com.xs0.dbktx.util.MockSQLConnection
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.SQLConnection
import kotlinx.coroutines.experimental.Unconfined
import kotlinx.coroutines.experimental.launch
import org.junit.Test

import java.util.concurrent.atomic.AtomicBoolean

import org.junit.Assert.*

class ExprFilterHasParentTest {
    @Test
    fun testParentQuery() {
        val called = AtomicBoolean(false)
        val connection = object : MockSQLConnection() {
            override fun queryWithParams(sql: String, params: JsonArray, resultHandler: Handler<AsyncResult<ResultSet>>): SQLConnection {
                assertEquals("SELECT company_id, key, name, tag_line, t_created, t_updated FROM brands WHERE company_id IN (SELECT id FROM companies WHERE name > ?)", sql)

                assertEquals(1, params.size().toLong())
                assertEquals("qwe", params.getString(0))

                called.set(true)
                return this
            }
        }
        val delayedExec = DelayedExec()
        val db = DbLoaderImpl(connection, delayedExec)

        launch(Unconfined) {
            db.run {
                TestSchema1.BRAND.query {
                    COMPANY_REF.has(TestSchema1.COMPANY) {
                        NAME gte "qwe"
                    }
                }
            }
        }

        assertTrue(called.get())
    }
}