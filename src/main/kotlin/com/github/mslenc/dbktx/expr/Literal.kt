package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql

class Literal<E, T: Any>(private val value: T, private val type: SqlType<T>) : Expr<E, T> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        type.toSql(value, sql)
    }

    override val couldBeNull: Boolean
        get() = false

    override fun getSqlType(): SqlType<T> {
        return type
    }

    override fun remap(remapper: TableRemapper): Expr<E, T> {
        return this
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}