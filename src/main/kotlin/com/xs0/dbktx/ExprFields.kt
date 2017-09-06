package com.xs0.dbktx

internal class ExprFields<E : DbEntity<E, *>, TYPE>(private val fieldsSql: String) : MultiValuedExpr<E, TYPE> {

    override fun toSql(sb: SqlBuilder, topLevel: Boolean) {
        sb.sql(fieldsSql)
    }
}
