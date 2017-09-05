package com.xs0.dbktx

class ExprOneOf<TABLE, T>(private val needle: Expr<in TABLE, T>, private val haystack: List<Expr<in TABLE, T>>) : ExprBoolean<TABLE> {
    init {
        if (haystack.isEmpty())
            throw IllegalArgumentException("Empty list for oneOf")
    }

    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.openParen(topLevel)

        needle.toSql(sb, false)
        sb.sql(" IN (")
        var i = 0
        val n = haystack.size
        while (i < n) {
            if (i > 0)
                sb.sql(", ")
            haystack[i].toSql(sb, true)
            i++
        }
        sb.sql(")")

        sb.closeParen(topLevel)
    }

    companion object {

        fun <TABLE, T> oneOf(needle: Expr<in TABLE, T>, haystack: List<Expr<in TABLE, T>>): ExprBoolean<TABLE> {
            return if (haystack.size == 1) {
                ExprEquals.equals(needle, haystack[0])
            } else {
                ExprOneOf(needle, haystack)
            }
        }
    }
}