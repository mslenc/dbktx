package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.util.Sql

class ExprLike<E> (
        private val value: Expr<in E, String>,
        private val pattern: Expr<in E, String>,
        private val escapeChar: Char = '|',
        private val negated: Boolean = false) : ExprBoolean {

    init {
        if (escapeChar == '\'')
            throw IllegalArgumentException("Invalid escape char - it can't be '")
    }

    override fun not(): ExprBoolean {
        return ExprLike(value, pattern, escapeChar, !negated)
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

    override fun remap(remapper: TableRemapper): ExprBoolean {
        return ExprLike(value.remap(remapper), pattern.remap(remapper), escapeChar, negated)
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}