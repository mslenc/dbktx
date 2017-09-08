package com.xs0.dbktx.util

import com.xs0.dbktx.schema.Column
import com.xs0.dbktx.schema.DbTable
import io.vertx.core.json.JsonArray
import java.time.Instant

class SqlBuilder {
    private val sql = StringBuilder()
    val params = JsonArray()

    fun getSql(): String {
        return sql.toString()
    }

    fun sql(sql: String): SqlBuilder {
        this.sql.append(sql)
        return this
    }

    fun param(param: Any): SqlBuilder {
        params.add(param)
        return this
    }

    fun param(param: String): SqlBuilder {
        params.add(param)
        return this
    }

    fun param(param: Int): SqlBuilder {
        params.add(param)
        return this
    }

    fun param(param: Long): SqlBuilder {
        params.add(param)
        return this
    }

    fun param(param: Double): SqlBuilder {
        params.add(param)
        return this
    }

    fun param(param: Float): SqlBuilder {
        params.add(param)
        return this
    }

    fun param(param: Boolean): SqlBuilder {
        params.add(param)
        return this
    }

    fun param(param: ByteArray): SqlBuilder {
        params.add(param)
        return this
    }

    fun param(param: Instant): SqlBuilder {
        params.add(param)
        return this
    }

    fun openParen(topLevel: Boolean): SqlBuilder {
        if (!topLevel)
            sql("(")
        return this
    }

    fun closeParen(topLevel: Boolean): SqlBuilder {
        if (!topLevel)
            sql(")")
        return this
    }

    fun name(column: Column<*, *>): SqlBuilder {
        sql.append(column.fieldName)
        return this
    }

    fun name(table: DbTable<*, *>): SqlBuilder {
        sql.append(table.dbName)
        return this
    }
}
