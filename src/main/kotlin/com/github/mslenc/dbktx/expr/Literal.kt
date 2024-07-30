package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql

class Literal<T: Any>(private val value: T, override val sqlType: SqlType<T>) : Expr<T> {
    override fun toSql(sql: Sql, nullWillBeFalse: Boolean, topLevel: Boolean) {
        sqlType.toSql(value, sql)
    }

    override val couldBeNull: Boolean
        get() = false

    override val involvesAggregation: Boolean
        get() = false

    override fun remap(remapper: TableRemapper): Expr<T> {
        return this
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}