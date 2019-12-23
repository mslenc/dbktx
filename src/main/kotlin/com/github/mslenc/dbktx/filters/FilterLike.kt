package com.github.mslenc.dbktx.filters

import com.github.mslenc.asyncdb.DbType
import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

class FilterLike (
        private val value: Expr<String>,
        private val pattern: Expr<String>,
        private val escapeChar: Char = '|',
        private val negated: Boolean = false,
        private val caseInsensitive: Boolean = false) : FilterExpr {

    init {
        require(escapeChar != '\'') { "Invalid escape char - it can't be '" }
    }

    override fun not(): FilterExpr {
        return FilterLike(value, pattern, escapeChar, !negated, caseInsensitive)
    }

    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            if (caseInsensitive) {
                if (sql.dbType == DbType.POSTGRES) {
                    + value
                    + (if (negated) " NOT ILIKE " else " ILIKE ")
                    + pattern
                    + " ESCAPE '"
                    + escapeChar.toString()
                    + "'"
                } else {
                    + "LOWER("
                    + value
                    + ")"
                    + (if (negated) " NOT LIKE " else " LIKE ")
                    + "LOWER("
                    + pattern
                    + ") ESCAPE '"
                    + escapeChar.toString()
                    + "'"
                }
            } else {
                + value
                + (if (negated) " NOT LIKE " else " LIKE ")
                + pattern
                + " ESCAPE '"
                + escapeChar.toString()
                + "'"
            }
        }
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterLike(value.remap(remapper), pattern.remap(remapper), escapeChar, negated, caseInsensitive)
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}