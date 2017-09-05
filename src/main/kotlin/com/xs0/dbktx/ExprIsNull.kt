package com.xs0.dbktx

class ExprIsNull<ENTITY>(private val inner: Expr<in ENTITY, *>) : ExprBoolean<ENTITY> {
    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.openParen(topLevel)
        inner.toSql(sb, false)
        sb.sql(" IS NULL")
        sb.closeParen(topLevel)
    }

    override fun not(): ExprBoolean<ENTITY> {
        return ExprIsNotNull(inner)
    }
}