package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql

class Literal<E, T: Any>(private val value: T, private val sqlType: SqlType<T>) : Expr<E, T> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sqlType.toSql(value, sql)
    }

    override fun remap(remapper: TableRemapper): Expr<E, T> {
        return this
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}