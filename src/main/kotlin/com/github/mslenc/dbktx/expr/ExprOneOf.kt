package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.util.Sql

class ExprOneOf<TABLE, T>(private val needle: Expr<TABLE, T>, private val haystack: List<Expr<TABLE, T>>, private val negated: Boolean = false) : ExprBoolean {
    init {
        if (haystack.isEmpty())
            throw IllegalArgumentException("Empty list for oneOf")
    }

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            +needle
            raw(if (negated) " NOT IN " else " IN ")
            paren { tuple(haystack) { +it } }
        }
    }

    override fun not(): ExprBoolean {
        return ExprOneOf(needle, haystack, !negated)
    }

    override fun remap(remapper: TableRemapper): ExprBoolean {
        return ExprOneOf(needle.remap(remapper), haystack.map { it.remap(remapper) }, negated)
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }

    companion object {
        fun <TABLE, T> oneOf(needle: Expr<TABLE, T>, haystack: List<Expr<TABLE, T>>): ExprBoolean {
            if (haystack.isEmpty())
                throw IllegalArgumentException("Empty list supplied to oneOf")

            if (haystack.size == 1)
                return ExprCmp(needle, ExprCmp.Op.EQ, haystack[0])

            return ExprOneOf(needle, haystack)
        }
    }
}