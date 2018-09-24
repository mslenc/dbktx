package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.util.Sql

class ExprNow<ENTITY, T> : Expr<ENTITY, T> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw("NOW()")
    }

    override val isComposite: Boolean
        get() = false

    override fun rangeTo(other: Expr<in ENTITY, T>): SqlRange<ENTITY, T> {
        throw UnsupportedOperationException("NOW() can't be used for comparisons")
    }

    override fun remap(remapper: TableRemapper): Expr<ENTITY, T> {
        return this
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}
