package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableInQuery
import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.schema.ColumnMapping
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql
import com.github.mslenc.dbktx.util.SqlBuilderHelpers
import java.lang.UnsupportedOperationException

internal class ExprFields<E : DbEntity<E, *>, TYPE : Any>(val columnMappings: Array<ColumnMapping<*, *, *>>, val tableInQuery: TableInQuery<E>) : Expr<TYPE> {
    override fun toSql(sql: Sql, nullWillBeFalse: Boolean, topLevel: Boolean) {
        sql.paren(true) {
            sql.tuple(columnMappings) {
                sql(SqlBuilderHelpers.forceBindFrom(it, tableInQuery), false, true)
            }
        }
    }

    override val couldBeNull: Boolean
        get() = columnMappings.any { it.columnFromAsNullable != null }

    override val involvesAggregation: Boolean
        get() = false // this is a tuple of column names and literals, so no aggregation possible

    override val sqlType: SqlType<TYPE>
        get() = throw UnsupportedOperationException("getSqlType called on ExprFields")

    override fun remap(remapper: TableRemapper): Expr<TYPE> {
        return ExprFields(columnMappings, remapper.remap(tableInQuery))
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}
