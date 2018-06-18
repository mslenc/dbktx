package com.xs0.dbktx.crud

import com.xs0.dbktx.expr.*
import com.xs0.dbktx.schema.*
import com.xs0.dbktx.sqltypes.SqlTypeVarchar
import com.xs0.dbktx.util.escapeSqlLikePattern

interface FilterBuilder<E: DbEntity<E, *>> {
    fun <T> bind(prop: RowProp<E, T>): Expr<E, T>

    infix fun <T> Expr<E, T>.eq(other: Expr<E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.EQ, other)
    }

    infix fun <T> RowProp<E, T>.eq(other: T): ExprBoolean<E> {
        return ExprBinary(bind(this), ExprBinary.Op.EQ, this.makeLiteral(other))
    }

    infix fun <T> Expr<E, T>.neq(other: Expr<E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.NEQ, other)
    }

    infix fun <T> RowProp<E, T>.neq(other: T): ExprBoolean<E> {
        return ExprBinary(bind(this), ExprBinary.Op.NEQ, this.makeLiteral(other))
    }

    infix fun <T> RowProp<E, T>.oneOf(values: Set<T>): ExprBoolean<E> {
        return when {
            values.isEmpty() ->
                throw IllegalArgumentException("Can't have empty set with oneOf")
            values.size == 1 ->
                eq(values.first())
            else ->
                oneOf(values.map { makeLiteral(it) })
        }
    }

    infix fun <T> oneOf(values: Iterable<T>): ExprBoolean<E> {
        return if (values is Set) {
            oneOf(values)
        } else {
            oneOf(values.toSet())
        }
    }

    infix fun <T> Expr<E, T>.lt(value: Expr<E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.LT, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lt(value: T): ExprBoolean<E> {
        return ExprBinary(bind(this), ExprBinary.Op.LT, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lt(value: Expr<E, T>): ExprBoolean<E> {
        return ExprBinary(bind(this), ExprBinary.Op.LT, value)
    }

    infix fun <T> Expr<E, T>.lte(value: Expr<E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.LTE, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lte(value: T): ExprBoolean<E> {
        return ExprBinary(bind(this), ExprBinary.Op.LTE, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lte(value: Expr<E, T>): ExprBoolean<E> {
        return ExprBinary(bind(this), ExprBinary.Op.LTE, value)
    }

    infix fun <T> Expr<E, T>.gt(value: Expr<E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.GT, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gt(value: T): ExprBoolean<E> {
        return ExprBinary(bind(this), ExprBinary.Op.GT, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gt(value: Expr<E, T>): ExprBoolean<E> {
        return ExprBinary(bind(this), ExprBinary.Op.GT, value)
    }

    infix fun <T> Expr<E, T>.gte(value: Expr<E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.GTE, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gte(value: T): ExprBoolean<E> {
        return ExprBinary(bind(this), ExprBinary.Op.GTE, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gte(value: Expr<E, T>): ExprBoolean<E> {
        return ExprBinary(bind(this), ExprBinary.Op.GTE, value)
    }

    fun <T : Comparable<T>> Expr<E, T>.between(minimum: Expr<E, T>, maximum: Expr<E, T>): ExprBoolean<E> {
        return ExprBetween(this, minimum, maximum, between = true)
    }

    fun <T : Comparable<T>> Expr<E, T>.notBetween(minimum: Expr<E, T>, maximum: Expr<E, T>): ExprBoolean<E> {
        return ExprBetween(this, minimum, maximum, between = false)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.between(range: ClosedRange<T>): ExprBoolean<E> {
        if (range.isEmpty())
            throw IllegalArgumentException("Can't have an empty range")

        return bind(this).between(makeLiteral(range.start), makeLiteral(range.endInclusive))
    }

    fun <T : Comparable<T>> OrderedProp<E, T>.between(minimum: T, maximum: T): ExprBoolean<E> {
        return bind(this).between(makeLiteral(minimum), makeLiteral(maximum))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.notBetween(range: ClosedRange<T>): ExprBoolean<E> {
        if (range.isEmpty())
            throw IllegalArgumentException("Can't have an empty range")

        return bind(this).notBetween(makeLiteral(range.start), makeLiteral(range.endInclusive))
    }

    fun <T : Comparable<T>> OrderedProp<E, T>.notBetween(minimum: T, maximum: T): ExprBoolean<E> {
        return bind(this).notBetween(makeLiteral(minimum), makeLiteral(maximum))
    }

    infix fun ExprString<E>.contains(value: String): ExprBoolean<E> {
        return like("%" + escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun ExprString<E>.startsWith(value: String): ExprBoolean<E> {
        return like(escapeSqlLikePattern(value, '|') + "%", '|')
    }

    infix fun ExprString<E>.endsWith(value: String): ExprBoolean<E> {
        return like("%" + escapeSqlLikePattern(value, '|'), '|')
    }

    infix fun ExprString<E>.like(pattern: String): ExprBoolean<E> {
        return like(pattern, '|')
    }

    infix fun ExprString<E>.like(pattern: Expr<in E, String>): ExprBoolean<E> {
        return like(pattern, '|')
    }

    fun ExprString<E>.like(pattern: String, escapeChar: Char): ExprBoolean<E> {
        return like(SqlTypeVarchar.makeLiteral(pattern), escapeChar)
    }

    fun ExprString<E>.like(pattern: Expr<in E, String>, escapeChar: Char): ExprBoolean<E> {
        return ExprLike(this, pattern, escapeChar)
    }

    infix fun <T> Expr<E, T>.oneOf(values: List<Expr<in E, T>>): ExprBoolean<E> {
        if (values.isEmpty())
            throw IllegalArgumentException("No possibilities specified")

        return ExprOneOf.oneOf(this, values)
    }

    operator fun ExprBoolean<E>.not(): ExprBoolean<E> {
        return ExprNegate(this)
    }

    infix fun ExprBoolean<E>.and(other: ExprBoolean<E>): ExprBoolean<E> {
        return ExprBools.create(this, ExprBools.Op.AND, other)
    }

    infix fun ExprBoolean<E>.or(other: ExprBoolean<E>): ExprBoolean<E> {
        return ExprBools.create(this, ExprBools.Op.OR, other)
    }

    fun <T> NOW(): ExprNow<E, T> {
        return ExprNow()
    }
}