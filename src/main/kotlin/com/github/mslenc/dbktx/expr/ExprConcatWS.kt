package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.sqltypes.SqlTypeVarchar
import com.github.mslenc.dbktx.util.Sql

class ExprConcatWS private constructor (private val sep: Expr<String>, private val parts: List<Expr<*>>) : Expr<String> {
    override fun toSql(sql: Sql, nullWillBeFalse: Boolean, topLevel: Boolean) {
        sql.raw("CONCAT_WS(")
            sql(sep, true, false)
            sql.raw(", ")

            sql.tuple(parts) {
                it.toSql(sql, false, true)
            }
        sql.raw(")")
    }

    override val couldBeNull: Boolean
        get() = sep.couldBeNull // nulls in parts are ignored by CONCAT_WS

    override val involvesAggregation: Boolean
        get() = sep.involvesAggregation || parts.any { it.involvesAggregation }

    override fun remap(remapper: TableRemapper): Expr<String> {
        return ExprConcatWS(sep.remap(remapper), parts.map { it.remap(remapper) })
    }

    override val sqlType: SqlType<String>
        get() {
            return when (sep) {
                is Literal -> sep.sqlType
                else -> SqlTypeVarchar.makeLiteral("").sqlType
            }
        }

    companion object {
        fun create(sep: Expr<String>, first: Expr<*>, parts: List<Expr<*>>): Expr<String> {
            val allParts = ArrayList<Expr<*>>()
            allParts.add(first)
            allParts.addAll(parts)

            return ExprConcatWS(sep, allParts)
        }
    }
}