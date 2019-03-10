package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

class FilterLike<E> (
        private val value: Expr<in E, String>,
        private val pattern: Expr<in E, String>,
        private val escapeChar: Char = '|',
        private val negated: Boolean = false) : FilterExpr {

    init {
        if (escapeChar == '\'')
            throw IllegalArgumentException("Invalid escape char - it can't be '")
    }

    override fun not(): FilterExpr {
        return FilterLike(value, pattern, escapeChar, !negated)
    }

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            + value
            + (if (negated) " NOT LIKE " else " LIKE ")
            + pattern
            + " ESCAPE '"
            + escapeChar.toString()
            + "'"
        }
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterLike(value.remap(remapper), pattern.remap(remapper), escapeChar, negated)
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}