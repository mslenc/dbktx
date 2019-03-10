package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.util.Sql

internal class ExprBinary<E: DbEntity<E, *>, T: Any>(private val left: Expr<in E, T>, private val op: Op, private val right: Expr<in E, T>): Expr<E, T> {
    internal enum class Op(internal val sql: String) {
        PLUS(" + "),
        MINUS(" - "),
        TIMES(" * "),
        DIV(" / "),
        REM(" % ")
    }

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +left
            +op.sql
            +right
        }
    }

    override fun remap(remapper: TableRemapper): Expr<E, T> {
        return ExprBinary(left.remap(remapper), op, right.remap(remapper))
    }
}