package com.xs0.dbktx

class ExprBetween<E, T>(
        private val value: Expr<in E, T>,
        private val minimum: Expr<in E, T>,
        private val maximum: Expr<in E, T>,
        private val between: Boolean
): ExprBoolean<E> {
    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.openParen(topLevel)

        value.toSql(sb, false)
        sb.sql(if (between) " BETWEEN " else " NOT BETWEEN ")
        minimum.toSql(sb, false)
        sb.sql(" AND ")
        maximum.toSql(sb, false)

        sb.closeParen(topLevel)
    }

    override fun not(): ExprBoolean<E> {
        return ExprBetween(value, minimum, maximum, !between)
    }
}
