package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql

internal class ExprBinary<T: Any>(private val left: Expr<T>, private val op: BinaryOp, private val right: Expr<T>): Expr<T> {
    override fun getSqlType(): SqlType<T> {
        return left.getSqlType()
    }

    override val couldBeNull: Boolean
        get() = left.couldBeNull || right.couldBeNull

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