package com.xs0.dbktx

class ExprOr<TABLE> private constructor(private val parts: List<Expr<in TABLE, Boolean>>) : ExprBoolean<TABLE> {

    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.openParen(topLevel)

        var i = 0
        val n = parts.size
        while (i < n) {
            if (i > 0)
                sb.sql(" OR ")
            parts[i].toSql(sb, false)
            i++
        }

        sb.closeParen(topLevel)
    }

    companion object {

        fun <TABLE> create(left: Expr<in TABLE, Boolean>, right: Expr<in TABLE, Boolean>): ExprOr<TABLE> {
            val parts = ArrayList<Expr<in TABLE, Boolean>>()

            if (left is ExprOr<*>) {
                val l = left as ExprOr<in TABLE>
                parts.addAll(l.parts)
            } else {
                parts.add(left)
            }

            if (right is ExprOr<*>) {
                val r = right as ExprOr<in TABLE>
                parts.addAll(r.parts)
            } else {
                parts.add(right)
            }

            return ExprOr(parts)
        }

        @SafeVarargs
        fun <TABLE> create(vararg exprs: Expr<in TABLE, Boolean>): Expr<in TABLE, Boolean>? {
            val parts = ArrayList<Expr<in TABLE, Boolean>>()

            for (expr in exprs) {
                if (expr is ExprOr<*>) {
                    parts.addAll((expr as ExprOr<in TABLE>).parts)
                } else {
                    parts.add(expr)
                }
            }

            if (parts.isEmpty())
                return null

            return if (parts.size == 1) parts[0] else ExprOr(parts)

        }
    }
}
