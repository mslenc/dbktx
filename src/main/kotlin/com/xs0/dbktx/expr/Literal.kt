package com.xs0.dbktx.expr

import com.xs0.dbktx.crud.TableRemapper
import com.xs0.dbktx.sqltypes.SqlType
import com.xs0.dbktx.util.Sql

class Literal<E, T: Any>(private val value: T, private val sqlType: SqlType<T>) : Expr<E, T> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sqlType.toSql(value, sql)
    }

    override fun remap(remapper: TableRemapper): Expr<E, T> {
        return this
    }
}