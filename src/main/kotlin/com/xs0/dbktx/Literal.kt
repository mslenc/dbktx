package com.xs0.dbktx

class Literal<E, T>(private val value: T, private val sqlType: Sqlable<T>) : Expr<E, T> {
    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sqlType.toSql(value, sb, topLevel)
    }
}