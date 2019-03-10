package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

internal class FilterCompare<E, T>(private val left: Expr<in E, T>, private val op: Op, private val right: Expr<in E, T>) : FilterExpr {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +left
            +op.sql
            +right
        }
    }

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
