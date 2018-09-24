package com.xs0.dbktx.expr

import com.xs0.dbktx.crud.TableInQuery
import com.xs0.dbktx.crud.TableRemapper
import com.xs0.dbktx.schema.ColumnMapping
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.util.Sql
import com.xs0.dbktx.util.SqlBuilderHelpers

internal class ExprFields<E : DbEntity<E, *>, TYPE>(val columnMappings: Array<ColumnMapping<*, *, *>>, val tableInQuery: TableInQuery<E>) : Expr<E, TYPE> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.paren(true) {
            sql.tuple(columnMappings) {
                +SqlBuilderHelpers.forceBindFrom(it, tableInQuery)
            }
        }
    }

    override fun remap(remapper: TableRemapper): Expr<E, TYPE> {
        return ExprFields(columnMappings, remapper.remap(tableInQuery))
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}
