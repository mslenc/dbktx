package com.xs0.dbktx.expr

import com.xs0.dbktx.util.Sql

class ExprBetween<E, T>(
        private val value: Expr<in E, T>,
        private val minimum: Expr<in E, T>,
        private val maximum: Expr<in E, T>,
        private val between: Boolean
): ExprBoolean<E> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +value
            +(if (between) " BETWEEN " else " NOT BETWEEN ")
            +minimum
            +" AND "
            +maximum
        }
    }

    override fun not(): ExprBoolean<E> {
        return ExprBetween(value, minimum, maximum, !between)
    }
}
