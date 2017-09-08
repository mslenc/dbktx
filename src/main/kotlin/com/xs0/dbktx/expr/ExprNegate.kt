package com.xs0.dbktx.expr

import com.xs0.dbktx.util.Sql

class ExprNegate<TABLE>(private val inner: ExprBoolean<TABLE>) : ExprBoolean<TABLE> {

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +"NOT "
            +inner
        }
    }

    override fun not(): ExprBoolean<TABLE> {
        return inner
    }
}