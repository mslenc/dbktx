package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql

internal enum class AggrExprOp {
    SUM,
    MIN,
    MAX,
    AVG
}

internal enum class CountExprOp {
    COUNT,
    COUNT_DISTINCT
}

internal class CountExpr<T : Any>(val op: CountExprOp, val expr: Expr<*>, override val sqlType: SqlType<T>) : NonNullAggrExpr<T>, SqlEmitter {
    override fun toSql(sql: Sql, nullWillBeFalse: Boolean, topLevel: Boolean) {
        sql.raw(when(op) {
            CountExprOp.COUNT -> "COUNT("
            CountExprOp.COUNT_DISTINCT -> "COUNT(DISTINCT "
        })
        expr.toSql(sql, false, true)
        sql.raw(")")
    }

    override fun remap(remapper: TableRemapper): Expr<T> {
        return CountExpr(op, expr.remap(remapper), sqlType)
    }
}

internal class AggrExprImpl<T : Any>(val op: AggrExprOp, val expr: Expr<*>, override val sqlType: SqlType<T>) : NullableAggrExpr<T>, SqlEmitter {
    override fun toSql(sql: Sql, nullWillBeFalse: Boolean, topLevel: Boolean) {
        sql.raw(when(op) {
            AggrExprOp.SUM -> "SUM("
            AggrExprOp.MIN -> "MIN("
            AggrExprOp.MAX -> "MAX("
            AggrExprOp.AVG -> "AVG("
        })
        expr.toSql(sql, false, true)
        sql.raw(")")
    }

    override fun remap(remapper: TableRemapper): Expr<T> {
        return AggrExprImpl(op, expr.remap(remapper), sqlType)
    }
}