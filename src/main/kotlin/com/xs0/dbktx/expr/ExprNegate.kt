package com.xs0.dbktx.expr

import com.xs0.dbktx.util.Sql

class ExprNegate(private val inner: ExprBoolean) : ExprBoolean {

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +"NOT "
            +inner
        }
    }

    override fun not(): ExprBoolean {
        return inner
    }
}