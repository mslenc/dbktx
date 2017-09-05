package com.xs0.dbktx

class ExprNegate<TABLE>(private val inner: Expr<in TABLE, Boolean>) : ExprBoolean<TABLE> {

    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.openParen(topLevel)

        sb.sql("NOT ")
        inner.toSql(sb, false)

        sb.closeParen(topLevel)
    }

    override fun not(): ExprBoolean<TABLE> {
        return if (inner is ExprBoolean<*>) {
            inner as ExprBoolean<TABLE>
        } else {
            ExprNegate(this)
        }
    }
}