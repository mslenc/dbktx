package com.xs0.dbktx.expr

import com.xs0.dbktx.sqltypes.SqlTypeVarchar
import com.xs0.dbktx.util.Sql
import com.xs0.dbktx.util.escapeSqlLikePattern

interface SqlEmitter {
    fun toSql(sql: Sql, topLevel: Boolean = false)
}

class SqlRange<E, T>(val minumum: Expr<in E, T>,
                     val maximum: Expr<in E, T>)

interface Expr<E, T> : SqlEmitter {
    infix fun eq(other: Expr<in E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.EQ, other)
    }

    infix fun `==`(other: Expr<in E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.EQ, other)
    }

    infix fun neq(other: Expr<in E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.NEQ, other)
    }

    infix fun oneOf(values: List<Expr<in E, T>>): ExprBoolean<E> {
        if (values.isEmpty())
            throw IllegalArgumentException("No possibilities specified")

        return ExprOneOf.oneOf(this, values)
    }

    operator fun rangeTo(other: Expr<in E, T>): SqlRange<E, T> {
        return SqlRange(this, other)
    }

    val isComposite: Boolean
        get() = false
}

interface NullableExpr<E, T> : Expr<E, T> {
    val isNull: ExprBoolean<E>
        get() = ExprIsNull(this, isNull = true)

    val isNotNull: ExprBoolean<E>
        get() = ExprIsNull(this, isNull = false)
}

interface NonNullExpr<E, T> : Expr<E, T>



interface OrderedExpr<E, T> : Expr<E, T> {
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
        return ExprBinary(this, ExprBinary.Op.GTE, value)
    }

    infix fun between(range: SqlRange<in E, T>): ExprBoolean<E> {
        return ExprBetween(this, range.minumum, range.maximum, between = true)
    }

    fun between(minimum: Expr<in E, T>, maximum: Expr<in E, T>): ExprBoolean<E> {
        return ExprBetween(this, minimum, maximum, between = true)
    }

    infix fun notBetween(range: SqlRange<in E, T>): ExprBoolean<E> {
        return ExprBetween(this, range.minumum, range.maximum, between = false)
    }

    fun notBetween(minimum: Expr<in E, T>, maximum: Expr<in E, T>): ExprBoolean<E> {
        return ExprBetween(this, minimum, maximum, between = false)
    }
}

interface NullableOrderedExpr<E, T>: OrderedExpr<E, T>, NullableExpr<E, T>
interface NonNullOrderedExpr<E, T>: OrderedExpr<E, T>, NonNullExpr<E, T>


interface ExprString<E> : OrderedExpr<E, String> {
    infix fun contains(value: String): ExprBoolean<E> {
        return like("%" + escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun startsWith(value: String): ExprBoolean<E> {
        return like(escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun endsWith(value: String): ExprBoolean<E> {
        return like("%" + escapeSqlLikePattern(value, '|'), '|')
    }

    infix fun like(pattern: String): ExprBoolean<E> {
        return like(pattern, '|')
    }

    infix fun like(pattern: Expr<in E, String>): ExprBoolean<E> {
        return like(pattern, '|')
    }

    fun like(pattern: String, escapeChar: Char): ExprBoolean<E> {
        return like(SqlTypeVarchar.makeLiteral(pattern), escapeChar)
    }

    fun like(pattern: Expr<in E, String>, escapeChar: Char): ExprBoolean<E> {
        return ExprLike(this, pattern, escapeChar)
    }
}

interface NullableExprString<E>: ExprString<E>, NullableExpr<E, String>
interface NonNullExprString<E>: ExprString<E>, NonNullExpr<E, String>


interface ExprBoolean<E> : SqlEmitter {
    operator fun not(): ExprBoolean<E> {
        return ExprNegate(this)
    }

    infix fun and(other: ExprBoolean<E>): ExprBoolean<E> {
        return ExprBools.create(this, ExprBools.Op.AND, other)
    }

    infix fun or(other: ExprBoolean<E>): ExprBoolean<E> {
        return ExprBools.create(this, ExprBools.Op.OR, other)
    }
}