package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql

class ExprNull<T : Any>(private val type: SqlType<T>) : Expr<T> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw("NULL")
    }

    override val couldBeNull: Boolean
        get() = true

    override fun getSqlType(): SqlType<T> {
        return type
    }

    override fun remap(remapper: TableRemapper): Expr<T> {
        return this
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }

    companion object {
        fun <T : Any> create(type: SqlType<T>): ExprNull<T> {
            return ExprNull(type)
        }
    }
}