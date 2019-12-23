package com.github.mslenc.dbktx.aggr

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.SqlEmitter
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql
import java.lang.IllegalStateException

internal enum class AggrOp {
    SUM,
    MIN,
    MAX,
    AVG,
    COUNT,
    COUNT_DISTINCT
}

internal class NonNullAggrExprImpl<E: DbEntity<E, *>, T : Any>(val op: AggrOp, val expr: Expr<*>, val columnIndex: Int, val sqlType: SqlType<T>) : NonNullAggrExpr<E, T>, SqlEmitter {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw(when(op) {
            AggrOp.COUNT -> "COUNT("
            AggrOp.COUNT_DISTINCT -> "COUNT(DISTINCT "
            else -> throw IllegalStateException("Aggregate $op is nullable, but used in NonNullAggrExpr")
        })
        expr.toSql(sql, true)
        sql.raw(")")
    }

    fun retrieveValue(row: DbRow): T {
        val value = row.getValue(columnIndex)
        return sqlType.parseDbValue(value)
    }
}

internal class NullableAggrExprImpl<E: DbEntity<E, *>, T : Any>(val op: AggrOp, val expr: Expr<*>, val columnIndex: Int, val sqlType: SqlType<T>) : NullableAggrExpr<E, T>, SqlEmitter {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw(when(op) {
            AggrOp.SUM -> "SUM("
            AggrOp.MIN -> "MIN("
            AggrOp.MAX -> "MAX("
            AggrOp.AVG -> "AVG("
            else -> throw IllegalStateException("Aggregate $op is non-null, but used in NullableAggrExpr")
        })
        expr.toSql(sql, true)
        sql.raw(")")
    }

    fun retrieveValue(row: DbRow): T? {
        val value = row.getValue(columnIndex)
        return if (value.isNull) {
            null
        } else {
            sqlType.parseDbValue(value)
        }
    }
}