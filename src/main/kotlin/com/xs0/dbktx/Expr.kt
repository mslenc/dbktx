package com.xs0.dbktx

import com.xs0.dbktx.sqltypes.SqlTypeVarchar


interface Expr<E, T> {
    fun toSql(sb: SqlBuilder, topLevel: Boolean)

    infix fun eq(other: Expr<in E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.EQ, other)
    }

    infix fun neq(other: Expr<in E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.NEQ, other)
    }

    val isComposite: Boolean
        get() = false
}

interface NullableExpr<E, T> : Expr<E, T> {
    val isNull: ExprBoolean<E>
        get() = ExprIsNull(this)

    val isNotNull: ExprBoolean<E>
        get() = ExprIsNotNull(this)
}

interface MultiValuedExpr<E, T> : Expr<E, T> {
    infix fun oneOf(values: List<Expr<in E, T>>): ExprBoolean<E> {
        return ExprOneOf.oneOf(this, values)
    }
}

interface OrderedExpr<E, T> : MultiValuedExpr<E, T> {
    infix fun lt(value: Expr<in E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.LT, value)
    }

    infix fun lte(value: Expr<in E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.LTE, value)
    }

    infix fun gt(value: Expr<in E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.GT, value)
    }

    infix fun gte(value: Expr<in E, T>): ExprBoolean<E> {
        return ExprBinary(this,  ExprBinary.Op.GTE, value)
    }
}

interface ExprString<E> : OrderedExpr<E, String> {
    fun contains(value: String): ExprBoolean<E> {
        return like("%" + escapePattern(value, '|') + "%", '|')
    }

    fun startsWith(value: String): ExprBoolean<E> {
        return like(escapePattern(value, '|') + "%", '|')
    }

    fun endsWith(value: String): ExprBoolean<E> {
        return like("%" + escapePattern(value, '|'), '|')
    }

    fun like(pattern: String, escapeChar: Char): ExprBoolean<E> {
        return like(SqlTypeVarchar.makeLiteral(pattern), escapeChar)
    }

    fun like(pattern: Expr<in E, String>, escapeChar: Char): ExprBoolean<E> {
        return ExprLike(this, pattern, escapeChar)
    }

    private fun escapePattern(pattern: String, escapeChar: Char): String {
        // lazy allocate, because most patterns are not expected to actually contain
        // _ or %
        var sb: StringBuilder? = null

        var i = 0
        val n = pattern.length
        while (i < n) {
            val c = pattern[i]
            if (c == '%' || c == '_' || c == escapeChar) {
                if (sb == null) {
                    sb = StringBuilder(pattern.length + 16)
                    sb.append(pattern, 0, i)
                }
                sb.append(escapeChar)
            }
            if (sb != null)
                sb.append(c)
            i++
        }

        return if (sb != null) sb.toString() else pattern
    }
}