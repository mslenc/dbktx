package com.xs0.dbktx

internal class ExprBinary<E, T>(private val left: Expr<in E, T>, private val op: Op, private val right: Expr<in E, T>) : ExprBoolean<E> {

    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.openParen(topLevel)

        left.toSql(sb, false)
        sb.sql(op.sql)
        right.toSql(sb, false)

        sb.closeParen(topLevel)
    }

    internal enum class Op(val sql: String) {
        LT(" < "),
        LTE(" <= "),
        GT(" > "),
        GTE(" >= "),
        EQ(" = "),
        NEQ(" <> ")
    }
}
