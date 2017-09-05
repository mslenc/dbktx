package com.xs0.dbktx

import java.util.ArrayList

class ExprAnd<TABLE> private constructor(private val parts: List<Expr<in TABLE, Boolean>>) : ExprBoolean<TABLE> {

    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.openParen(topLevel)

        var i = 0
        val n = parts.size
        while (i < n) {
            if (i > 0)
                sb.sql(" AND ")
            parts[i].toSql(sb, false)
            i++
        }

        sb.closeParen(topLevel)
    }

    companion object {

        fun <TABLE> create(left: Expr<in TABLE, Boolean>, right: Expr<in TABLE, Boolean>): ExprAnd<TABLE> {
            val parts = ArrayList<Expr<in TABLE, Boolean>>()

            if (left is ExprAnd<*>) {
                val l = left as ExprAnd<in TABLE>
                parts.addAll(l.parts)
            } else {
                parts.add(left)
            }

            if (right is ExprAnd<*>) {
                val r = right as ExprAnd<in TABLE>
                parts.addAll(r.parts)
            } else {
                parts.add(right)
            }

            return ExprAnd(parts)
        }
    }
}