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
        for ((i, sub) in haystack.withIndex()) {
            if (i > 0)
                sb.sql(", ")
            sub.toSql(sb, true)
        }
        sb.sql(")")

        sb.closeParen(topLevel)
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