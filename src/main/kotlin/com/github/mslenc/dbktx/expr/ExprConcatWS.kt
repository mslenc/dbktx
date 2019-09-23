package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.sqltypes.SqlTypeVarchar
import com.github.mslenc.dbktx.util.Sql

class ExprConcatWS<E> private constructor (private val sep: Expr<E, String>, private val parts: List<Expr<E, *>>) : Expr<E, String> {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw("CONCAT_WS(")
            sep.toSql(sql)
            sql.raw(", ")

            sql.tuple(parts) {
                it.toSql(sql, true)
            }
        sql.raw(")")
    }

    override fun remap(remapper: TableRemapper): Expr<E, String> {
        return ExprConcatWS(sep.remap(remapper), parts.map { it.remap(remapper) })
    }

    override fun getSqlType(): SqlType<String> {
        return when (sep) {
            is Literal -> sep.getSqlType()
            else -> SqlTypeVarchar.makeLiteral<E>("").getSqlType()
        }
    }

    companion object {
        fun <E> create(sep: Expr<E, String>, first: Expr<E, *>, parts: List<Expr<E, *>>): Expr<E, String> {
            val allParts = ArrayList<Expr<E, *>>()
            allParts.add(first)
            allParts.addAll(parts)

            return ExprConcatWS(sep, allParts)
        }
    }
}