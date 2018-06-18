package com.xs0.dbktx.expr

import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.util.Sql

class ExprNow<ENTITY, T> : Expr<ENTITY, T> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw("NOW()")
    }

    override val isComposite: Boolean
        get() = false

    override fun eq(other: Expr<in ENTITY, T>): ExprBoolean<ENTITY> {
        throw UnsupportedOperationException("NOW() can't be used for comparisons")
    }

    override fun `==`(other: Expr<in ENTITY, T>): ExprBoolean<ENTITY> {
        throw UnsupportedOperationException("NOW() can't be used for comparisons")
    }

    override fun neq(other: Expr<in ENTITY, T>): ExprBoolean<ENTITY> {
        throw UnsupportedOperationException("NOW() can't be used for comparisons")
    }

    override fun oneOf(values: List<Expr<in ENTITY, T>>): ExprBoolean<ENTITY> {
        throw UnsupportedOperationException("NOW() can't be used for comparisons")
    }

    override fun rangeTo(other: Expr<in ENTITY, T>): SqlRange<ENTITY, T> {
        throw UnsupportedOperationException("NOW() can't be used for comparisons")
    }
}
