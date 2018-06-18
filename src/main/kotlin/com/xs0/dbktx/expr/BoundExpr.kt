package com.xs0.dbktx.expr

import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.util.Sql

class BoundExpr<E: DbEntity<E, *>, T>(val tableInQuery: TableInQuery<E>, val innerExpr: Expr<E, T>) : Expr<E, T>, SqlEmitter {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.withTable(tableInQuery) {
            innerExpr.toSql(this, topLevel)
        }
    }
}