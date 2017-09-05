package com.xs0.dbktx

class ExprEquals<TABLE, T>(private val left: Expr<in TABLE, T>, private val equals: Boolean, private val right: Expr<in TABLE, T>) : ExprBoolean<TABLE> {

    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.openParen(topLevel)

        left.toSql(sb, false)
        sb.sql(if (equals) " = " else " <> ")
        right.toSql(sb, false)

        sb.closeParen(topLevel)
    }

    companion object {

        fun <TABLE, T> equals(left: Expr<in TABLE, T>, right: Expr<in TABLE, T>): ExprBoolean<TABLE> {
            return ExprEquals(left, true, right)
        }

        fun <TABLE, T> notEquals(left: Expr<in TABLE, T>, right: Expr<in TABLE, T>): ExprBoolean<TABLE> {
            return ExprEquals(left, false, right)
        }
    }
}
