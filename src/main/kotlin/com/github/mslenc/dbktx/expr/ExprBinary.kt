package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql

enum class BinaryOp(val sql: String) {
    PLUS("+"),
    MINUS("-"),
    TIMES("*"),
    DIV("/"),
    REM("%")
}

internal class ExprBinary<T: Any>(private val left: Expr<T>, private val op: BinaryOp, private val right: Expr<T>): Expr<T> {
    override val sqlType: SqlType<T>
        get() = left.sqlType

    override val couldBeNull: Boolean
        get() = left.couldBeNull || right.couldBeNull

    override val involvesAggregation: Boolean
        get() = left.involvesAggregation || right.involvesAggregation

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +left
            +" "
            +op.sql
            +" "
            +right
        }
    }

    override fun remap(remapper: TableRemapper): Expr<T> {
        return ExprBinary(left.remap(remapper), op, right.remap(remapper))
    }
}