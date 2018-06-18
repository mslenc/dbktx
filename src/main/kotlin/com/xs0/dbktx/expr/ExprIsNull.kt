package com.xs0.dbktx.expr

import com.xs0.dbktx.util.Sql

class ExprIsNull<ENTITY>(private val inner: Expr<in ENTITY, *>, private val isNull: Boolean) : ExprBoolean {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +inner
            if (isNull) {
                raw(" IS NULL")
            } else {
                raw(" IS NOT NULL")
            }
        }
    }

    override fun not(): ExprBoolean {
        return ExprIsNull(inner, !isNull)
    }
}
