package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql

internal class ExprBinary<E: DbEntity<E, *>, T: Any>(private val left: Expr<in E, T>, private val op: BinaryOp, private val right: Expr<in E, T>): Expr<E, T> {
    override fun getSqlType(): SqlType<T> {
        return left.getSqlType()
    }

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +left
            +" "
            +op.sql
            +" "
            +right
        }
    }

    override fun remap(remapper: TableRemapper): Expr<E, T> {
        return ExprBinary(left.remap(remapper), op, right.remap(remapper))
    }
}