package com.xs0.dbktx.expr

import com.xs0.dbktx.util.SqlBuilder

class ExprBools<E> private constructor(private val parts: List<ExprBoolean<E>>, private val op: Op) : ExprBoolean<E> {
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
        internal fun <E> create(left: ExprBoolean<E>, op: Op, right: ExprBoolean<E>): ExprBools<E> {
            val parts = ArrayList<ExprBoolean<E>>()

            if (left is ExprBools && left.op == op) {
                parts.addAll(left.parts)
            } else {
                parts.add(left)
            }

            if (right is ExprBools && right.op == op) {
                parts.addAll(right.parts)
            } else {
                parts.add(right)
            }

            return ExprBools(parts, op)
        }
    }
}