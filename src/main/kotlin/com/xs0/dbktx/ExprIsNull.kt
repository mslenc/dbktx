package com.xs0.dbktx

class ExprIsNull<ENTITY>(private val inner: Expr<in ENTITY, *>, private val isNull: Boolean) : ExprBoolean<ENTITY> {
    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.openParen(topLevel)
        inner.toSql(sb, false)
        sb.sql(if (isNull) " IS NULL" else " IS NOT NULL")
        sb.closeParen(topLevel)
    }

    override fun not(): ExprBoolean<ENTITY> {
        return ExprIsNull(inner, !isNull)
    }
}
