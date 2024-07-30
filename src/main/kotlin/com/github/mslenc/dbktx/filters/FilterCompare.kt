package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

internal class FilterCompare<T: Any>(private val left: Expr<T>, private val op: Op, private val right: Expr<T>) : FilterExpr {
    override fun toSql(sql: Sql, nullWillBeFalse: Boolean, topLevel: Boolean) {
        sql.expr(topLevel) {
            sql(left, nullWillBeFalse, false)
            +op.sql
            sql(right, nullWillBeFalse, false)
        }
    }

    override val couldBeNull: Boolean
        get() = left.couldBeNull || right.couldBeNull

    override val involvesAggregation: Boolean
        get() = left.involvesAggregation || right.involvesAggregation

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterCompare(left.remap(remapper), op, right.remap(remapper))
    }

    internal enum class Op(val sql: String) {
        LT(" < "),
        LTE(" <= "),
        GT(" > "),
        GTE(" >= "),
        EQ(" = "),
        NEQ(" <> ")
    }

    override fun not(): FilterExpr {
        return when (op) {
            Op.LT -> FilterCompare(left, Op.GTE, right)
            Op.LTE -> FilterCompare(left, Op.GT, right)
            Op.GT -> FilterCompare(left, Op.LTE, right)
            Op.GTE -> FilterCompare(left, Op.LT, right)
            Op.EQ -> FilterCompare(left, Op.NEQ, right)
            Op.NEQ -> FilterCompare(left, Op.EQ, right)
        }
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}
