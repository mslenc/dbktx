package com.xs0.dbktx.expr

import com.xs0.dbktx.util.Sql

class ExprOneOf<TABLE, T>(private val needle: Expr<in TABLE, T>, private val haystack: List<Expr<in TABLE, T>>) : ExprBoolean<TABLE> {
    init {
        if (haystack.isEmpty())
            throw IllegalArgumentException("Empty list for oneOf")
    }

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +needle
            +" IN "
            paren { tuple(haystack) { +it } }
        }
    }

    companion object {
        fun <TABLE, T> oneOf(needle: Expr<in TABLE, T>, haystack: List<Expr<in TABLE, T>>): ExprBoolean<TABLE> {
            if (haystack.isEmpty())
                throw IllegalArgumentException("Empty list supplied to oneOf")

            if (haystack.size == 1)
                return ExprBinary(needle, ExprBinary.Op.EQ, haystack[0])

            return ExprOneOf(needle, haystack)
        }
    }
}