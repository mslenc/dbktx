package com.xs0.dbktx.crud

import com.xs0.dbktx.expr.*
import com.xs0.dbktx.schema.*

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

    infix fun <T> Expr<E, T>.lte(value: Expr<E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.LTE, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.lte(value: T): ExprBoolean<E> {
        return ExprBinary(bind(this), ExprBinary.Op.LTE, makeLiteral(value))
    }

    infix fun <T> Expr<E, T>.gt(value: Expr<E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.GT, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gt(value: T): ExprBoolean<E> {
        return ExprBinary(bind(this), ExprBinary.Op.GT, makeLiteral(value))
    }

    infix fun <T> Expr<E, T>.gte(value: Expr<E, T>): ExprBoolean<E> {
        return ExprBinary(this, ExprBinary.Op.GTE, value)
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.gte(value: T): ExprBoolean<E> {
        return ExprBinary(bind(this), ExprBinary.Op.GTE, makeLiteral(value))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.between(range: ClosedRange<T>): ExprBoolean<E> {
        if (range.isEmpty())
            throw IllegalArgumentException("Can't have an empty range")

        return between(makeLiteral(range.start), makeLiteral(range.endInclusive))
    }

    fun <T : Comparable<T>> OrderedProp<E, T>.between(minimum: T, maximum: T): ExprBoolean<E> {
        return between(makeLiteral(minimum), makeLiteral(maximum))
    }

    infix fun <T : Comparable<T>> OrderedProp<E, T>.notBetween(range: ClosedRange<T>): ExprBoolean<E> {
        if (range.isEmpty())
            throw IllegalArgumentException("Can't have an empty range")

        return notBetween(makeLiteral(range.start), makeLiteral(range.endInclusive))
    }

    fun <T : Comparable<T>> OrderedProp<E, T>.notBetween(minimum: T, maximum: T): ExprBoolean<E> {
        return notBetween(makeLiteral(minimum), makeLiteral(maximum))
    }
}