package com.xs0.dbktx.expr

import com.xs0.dbktx.crud.TableRemapper
import com.xs0.dbktx.util.Sql

internal class ExprBinary<E, T>(private val left: Expr<in E, T>, private val op: Op, private val right: Expr<in E, T>) : ExprBoolean {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +left
            +op.sql
            +right
        }
    }

    override fun remap(remapper: TableRemapper): ExprBoolean {
        return ExprBinary(left.remap(remapper), op, right.remap(remapper))
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
            ExprBinary.Op.LT -> ExprBinary(left, Op.GTE, right)
            ExprBinary.Op.LTE -> ExprBinary(left, Op.GT, right)
            ExprBinary.Op.GT -> ExprBinary(left, Op.LTE, right)
            ExprBinary.Op.GTE -> ExprBinary(left, Op.LT, right)
            ExprBinary.Op.EQ -> ExprBinary(left, Op.NEQ, right)
            ExprBinary.Op.NEQ -> ExprBinary(left, Op.EQ, right)
        }
    }
}
