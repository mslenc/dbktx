package com.xs0.dbktx.expr

import com.xs0.dbktx.util.Sql

class ExprLike<E> (
        private val value: Expr<in E, String>,
        private val pattern: Expr<in E, String>,
        private val escapeChar: Char = '|') : ExprBoolean<E> {

    init {
        if (escapeChar == '\'')
            throw IllegalArgumentException("Invalid escape char - it can't be '")
    }

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            + value
            + " LIKE "
            + pattern
            + " ESCAPE '"
            + escapeChar.toString()
            + "'"
        }
    }
}