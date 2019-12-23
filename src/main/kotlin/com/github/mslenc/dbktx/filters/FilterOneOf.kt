package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

class FilterOneOf<T: Any>(private val needle: Expr<T>, private val haystack: List<Expr<T>>, private val negated: Boolean = false) : FilterExpr {
    init {
        if (haystack.isEmpty())
            throw IllegalArgumentException("Empty list for oneOf")
    }

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            sql.inLiteralSetWrapper(negated, needleCanBeNull = needle.couldBeNull) { IN ->
                +needle
                +IN
                paren { tuple(haystack) { +it } }
            }
        }
    }

    override fun not(): FilterExpr {
        return FilterOneOf(needle, haystack, !negated)
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterOneOf(needle.remap(remapper), haystack.map { it.remap(remapper) }, negated)
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }

    companion object {
        fun <T: Any> oneOf(needle: Expr<T>, haystack: List<Expr<T>>): FilterExpr {
            if (haystack.isEmpty())
                return MatchNothing

            if (haystack.size == 1)
                return FilterCompare(needle, FilterCompare.Op.EQ, haystack[0])

            return FilterOneOf(needle, haystack)
        }
    }
}