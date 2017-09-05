package com.xs0.dbktx

class ExprIsNotNull<ENTITY>(private val inner: Expr<in ENTITY, *>) : ExprBoolean<ENTITY> {
    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.openParen(topLevel)
        inner.toSql(sb, false)
        sb.sql(" IS NOT NULL")
        sb.closeParen(topLevel)
    }

    override fun not(): ExprBoolean<ENTITY> {
        return ExprIsNull(inner)
    }
}
