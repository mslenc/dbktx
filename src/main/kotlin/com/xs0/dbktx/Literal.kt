package com.xs0.dbktx

class Literal<T>(private val value: T?, private val sqlType: Sqlable<T>) : Expr<Any, T> {
    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        if (value == null) {
            sb.sql("NULL")
        } else {
            sqlType.toSql(value, sb, topLevel)
        }
    }
}