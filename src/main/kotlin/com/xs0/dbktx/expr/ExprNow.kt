package com.xs0.dbktx.expr

import com.xs0.dbktx.crud.TableRemapper
import com.xs0.dbktx.util.Sql

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
}
