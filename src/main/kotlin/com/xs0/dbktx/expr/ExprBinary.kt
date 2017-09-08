package com.xs0.dbktx.expr

import com.xs0.dbktx.util.Sql

internal class ExprBinary<E, T>(private val left: Expr<in E, T>, private val op: Op, private val right: Expr<in E, T>) : ExprBoolean<E> {

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +left
            +op.sql
            +right
        }
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
