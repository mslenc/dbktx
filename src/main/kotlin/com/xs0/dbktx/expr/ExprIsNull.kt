package com.xs0.dbktx.expr

import com.xs0.dbktx.util.SqlBuilder

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
