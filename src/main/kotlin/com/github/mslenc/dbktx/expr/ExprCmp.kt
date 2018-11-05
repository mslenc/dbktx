package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.util.Sql

internal class ExprCmp<E, T>(private val left: Expr<in E, T>, private val op: Op, private val right: Expr<in E, T>) : ExprBoolean {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +left
            +op.sql
            +right
        }
    }

    override fun remap(remapper: TableRemapper): ExprBoolean {
        return ExprCmp(left.remap(remapper), op, right.remap(remapper))
    }

    internal enum class Op(val sql: String) {
        LT(" < "),
        LTE(" <= "),
        GT(" > "),
        GTE(" >= "),
        EQ(" = "),
        NEQ(" <> ")
    }

    override fun not(): ExprBoolean {
        return when (op) {
            ExprCmp.Op.LT -> ExprCmp(left, Op.GTE, right)
            ExprCmp.Op.LTE -> ExprCmp(left, Op.GT, right)
            ExprCmp.Op.GT -> ExprCmp(left, Op.LTE, right)
            ExprCmp.Op.GTE -> ExprCmp(left, Op.LT, right)
            ExprCmp.Op.EQ -> ExprCmp(left, Op.NEQ, right)
            ExprCmp.Op.NEQ -> ExprCmp(left, Op.EQ, right)
        }
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}
