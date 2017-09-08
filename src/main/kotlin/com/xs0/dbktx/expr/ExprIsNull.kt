package com.xs0.dbktx.expr

import com.xs0.dbktx.util.Sql

class ExprIsNull<ENTITY>(private val inner: Expr<in ENTITY, *>, private val isNull: Boolean) : ExprBoolean<ENTITY> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +inner
            if (isNull) {
                +" IS NULL"
            } else {
                +" IS NOT NULL"
            }
        }
    }

    override fun not(): ExprBoolean<ENTITY> {
        return ExprIsNull(inner, !isNull)
    }
}
