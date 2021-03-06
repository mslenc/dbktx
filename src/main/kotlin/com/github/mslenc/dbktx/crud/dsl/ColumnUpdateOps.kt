package com.github.mslenc.dbktx.crud.dsl

import com.github.mslenc.dbktx.expr.BinaryOp
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.ExprBinary
import com.github.mslenc.dbktx.expr.ExprConcatWS
import com.github.mslenc.dbktx.schema.Column
import com.github.mslenc.dbktx.schema.DbEntity

interface ColumnUpdateOps<E: DbEntity<E, *>, T: Any> {
    fun literal(value: T): Expr<T>
    fun bind(column: Column<E, T>): Expr<T>

    operator fun Column<E, T>.unaryPlus() = bind(this)

    operator fun Expr<T>.plus(other: Expr<T>): Expr<T> = ExprBinary(this, BinaryOp.PLUS, other)
    operator fun Expr<T>.plus(other: Column<E, T>) = this + bind(other)
    operator fun Expr<T>.plus(other: T) = this + literal(other)
    operator fun Column<E, T>.plus(other: T) = bind(this) + literal(other)
    operator fun Column<E, T>.plus(other: Expr<T>) = bind(this) + other
    operator fun Column<E, T>.plus(other: Column<E, T>) = bind(this) + bind(other)
    operator fun T.plus(other: Expr<T>) = literal(this) + other
    operator fun T.plus(other: Column<E, T>) = literal(this) + bind(other)

    operator fun Expr<T>.minus(other: Expr<T>): Expr<T> = ExprBinary(this, BinaryOp.MINUS, other)
    operator fun Expr<T>.minus(other: Column<E, T>) = this - bind(other)
    operator fun Expr<T>.minus(other: T) = this - literal(other)
    operator fun Column<E, T>.minus(other: T) = bind(this) - literal(other)
    operator fun Column<E, T>.minus(other: Expr<T>) = bind(this) - other
    operator fun Column<E, T>.minus(other: Column<E, T>) = bind(this) - bind(other)
    operator fun T.minus(other: Expr<T>) = literal(this) - other
    operator fun T.minus(other: Column<E, T>) = literal(this) - bind(other)

    operator fun Expr<T>.times(other: Expr<T>): Expr<T> = ExprBinary(this, BinaryOp.TIMES, other)
    operator fun Expr<T>.times(other: Column<E, T>) = this * bind(other)
    operator fun Expr<T>.times(other: T) = this * literal(other)
    operator fun Column<E, T>.times(other: T) = bind(this) * literal(other)
    operator fun Column<E, T>.times(other: Expr<T>) = bind(this) * other
    operator fun Column<E, T>.times(other: Column<E, T>) = bind(this) * bind(other)
    operator fun T.times(other: Expr<T>) = literal(this) * other
    operator fun T.times(other: Column<E, T>) = literal(this) * bind(other)

    operator fun Expr<T>.div(other: Expr<T>): Expr<T> = ExprBinary(this, BinaryOp.DIV, other)
    operator fun Expr<T>.div(other: Column<E, T>) = this / bind(other)
    operator fun Expr<T>.div(other: T) = this / literal(other)
    operator fun Column<E, T>.div(other: T) = bind(this) / literal(other)
    operator fun Column<E, T>.div(other: Expr<T>) = bind(this) / other
    operator fun Column<E, T>.div(other: Column<E, T>) = bind(this) / bind(other)
    operator fun T.div(other: Expr<T>) = literal(this) / other
    operator fun T.div(other: Column<E, T>) = literal(this) / bind(other)

    operator fun Expr<T>.rem(other: Expr<T>): Expr<T> = ExprBinary(this, BinaryOp.REM, other)
    operator fun Expr<T>.rem(other: Column<E, T>) = this % bind(other)
    operator fun Expr<T>.rem(other: T) = this % literal(other)
    operator fun Column<E, T>.rem(other: T) = bind(this) % literal(other)
    operator fun Column<E, T>.rem(other: Expr<T>) = bind(this) % other
    operator fun Column<E, T>.rem(other: Column<E, T>) = bind(this) % bind(other)
    operator fun T.rem(other: Expr<T>) = literal(this) % other
    operator fun T.rem(other: Column<E, T>) = literal(this) % bind(other)
}

fun <E: DbEntity<E, *>> ColumnUpdateOps<E, String>.concatWs(sep: String, first: Expr<*>, vararg rest: Expr<*>): Expr<String> {
    return ExprConcatWS.create(literal(sep), first, rest.toList())
}