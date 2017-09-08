package com.xs0.dbktx.expr

import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.util.SqlBuilder

internal class ExprFields<E : DbEntity<E, *>, TYPE>(private val fieldsSql: String) : Expr<E, TYPE> {
    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.sql(fieldsSql)
    }
}
