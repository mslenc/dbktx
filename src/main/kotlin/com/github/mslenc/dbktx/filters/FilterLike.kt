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

    override val couldBeNull: Boolean
        get() = value.couldBeNull || pattern.couldBeNull

    override val involvesAggregation: Boolean
        get() = value.involvesAggregation || pattern.involvesAggregation

    override fun not(): FilterExpr {
        return FilterLike(value, pattern, escapeChar, !negated, caseInsensitive)
    }

    override fun toSql(sql: Sql, nullWillBeFalse: Boolean, topLevel: Boolean) {
        sql.expr(topLevel) {
            if (caseInsensitive) {
                if (sql.dbType == DbType.POSTGRES) {
                    sql(value, false, false)
                    + (if (negated) " NOT ILIKE " else " ILIKE ")
                    sql(pattern, false, false)
                    + " ESCAPE '"
                    + escapeChar.toString()
                    + "'"
                } else {
                    + "LOWER("
                    sql(value, false, false)
                    + ")"
                    + (if (negated) " NOT LIKE " else " LIKE ")
                    + "LOWER("
                    sql(pattern, false, false)
                    + ") ESCAPE '"
                    + escapeChar.toString()
                    + "'"
                }
            } else {
                sql(value, false, false)
                + (if (negated) " NOT LIKE " else " LIKE ")
                sql(pattern, false, false)
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