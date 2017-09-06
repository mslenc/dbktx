package com.xs0.dbktx

import java.util.ArrayList

class ExprBools<E> private constructor(private val parts: List<Expr<in E, Boolean>>, private val op: Op) : ExprBoolean<E> {
    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.openParen(topLevel)

        var i = 0
        val n = parts.size
        while (i < n) {
            if (i > 0)
                sb.sql(op.sql)
            parts[i].toSql(sb, false)
            i++
        }

        sb.closeParen(topLevel)
    }

    internal enum class Op(internal val sql: String) {
        AND(" AND "),
        OR(" OR ")
    }

    companion object {
        internal fun <E> create(left: Expr<in E, Boolean>, op: Op, right: Expr<in E, Boolean>): ExprBools<E> {
            val parts = ArrayList<Expr<in E, Boolean>>()

            if (left is ExprBools<*> && left.op == op) {
                @Suppress("UNCHECKED_CAST")
                val cast = left as ExprBools<E>
                for (part in cast.parts)
                    parts.add(part)
            } else {
                parts.add(left)
            }

            if (right is ExprBools<*> && right.op == op) {
                @Suppress("UNCHECKED_CAST")
                val cast = right as ExprBools<E> // to silence the compiler..
                for (part in cast.parts)
                    parts.add(part)
            } else {
                parts.add(right)
            }

            return ExprBools(parts, op)
        }
    }
}