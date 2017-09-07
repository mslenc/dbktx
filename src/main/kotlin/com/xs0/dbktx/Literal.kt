package com.xs0.dbktx

import com.xs0.dbktx.sqltypes.SqlType

class Literal<E, T: Any>(private val value: T, private val sqlType: SqlType<T>) : Expr<E, T> {
    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sqlType.toSql(value, sb, topLevel)
    }
}