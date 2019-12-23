package com.github.mslenc.dbktx.aggr

import com.github.mslenc.dbktx.expr.BinaryOp
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.Literal
import com.github.mslenc.dbktx.schema.*

interface RelPath<BASE: DbEntity<BASE, *>, LAST: DbEntity<LAST, *>>

interface AggrExprBuilder<E: DbEntity<E, *>> {
    fun <T: Any> Column<E, T>.itself(): Expr<T>

    operator fun <T: Any> Column<E, T>.unaryPlus(): Expr<T> {
        return this.itself()
    }

    operator fun <MID: DbEntity<MID, *>, T: Any> RelToOne<E, MID>.rangeTo(column: Column<MID, T>): Expr<T>
    operator fun <MID: DbEntity<MID, *>, T: Any> RelToMany<E, MID>.rangeTo(column: Column<MID, T>): Expr<T>
    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>> RelToOne<E, MID>.rangeTo(relToOne: RelToOne<MID, NEXT>): RelPath<E, NEXT>
    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>> RelToOne<E, MID>.rangeTo(relToMany: RelToMany<MID, NEXT>): RelPath<E, NEXT>
    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>> RelToMany<E, MID>.rangeTo(relToOne: RelToOne<MID, NEXT>): RelPath<E, NEXT>
    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>> RelToMany<E, MID>.rangeTo(relToMany: RelToMany<MID, NEXT>): RelPath<E, NEXT>
    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>> RelPath<E, MID>.rangeTo(relToOne: RelToOne<MID, NEXT>): RelPath<E, NEXT>
    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>> RelPath<E, MID>.rangeTo(relToMany: RelToMany<MID, NEXT>): RelPath<E, NEXT>
    operator fun <MID: DbEntity<MID, *>, T: Any> RelPath<E, MID>.rangeTo(column: Column<MID, T>): Expr<T>

    fun <T: Any> binary(left: Expr<T>, op: BinaryOp, right: Expr<T>): Expr<T>

    operator fun <T: Any> Expr<T>.  plus(other: Expr<T>  ) = binary(this, BinaryOp.PLUS, other)
    operator fun <T: Any> Expr<T>.  plus(other: Column<E, T>) = this + +other
    operator fun <T: Any> Expr<T>.  plus(other: T           ) = this + makeLiteral(other)
    operator fun <T: Any> Column<E, T>.plus(other: Expr<T>  ) = +this + other
    operator fun <T: Any> Column<E, T>.plus(other: Column<E, T>) = +this + +other
    operator fun <T: Any> Column<E, T>.plus(other: T           ) = +this + makeLiteral(other)
    operator fun <T: Any> T.           plus(other: Expr<T>  ) = other.makeLiteral(this) + other
    operator fun <T: Any> T.           plus(other: Column<E, T>) = other.makeLiteral(this) + other

    operator fun <T: Any> Expr<T>.  minus(other: Expr<T>  ) = binary(this, BinaryOp.MINUS, other)
    operator fun <T: Any> Expr<T>.  minus(other: Column<E, T>) = this - +other
    operator fun <T: Any> Expr<T>.  minus(other: T           ) = this - makeLiteral(other)
    operator fun <T: Any> Column<E, T>.minus(other: Expr<T>  ) = +this - other
    operator fun <T: Any> Column<E, T>.minus(other: Column<E, T>) = +this - +other
    operator fun <T: Any> Column<E, T>.minus(other: T           ) = +this - makeLiteral(other)
    operator fun <T: Any> T.           minus(other: Expr<T>  ) = other.makeLiteral(this) - other
    operator fun <T: Any> T.           minus(other: Column<E, T>) = other.makeLiteral(this) - other

    operator fun <T: Any> Expr<T>.  times(other: Expr<T>  ) = binary(this, BinaryOp.TIMES, other)
    operator fun <T: Any> Expr<T>.  times(other: Column<E, T>) = this * +other
    operator fun <T: Any> Expr<T>.  times(other: T           ) = this * makeLiteral(other)
    operator fun <T: Any> Column<E, T>.times(other: Expr<T>  ) = +this * other
    operator fun <T: Any> Column<E, T>.times(other: Column<E, T>) = +this * +other
    operator fun <T: Any> Column<E, T>.times(other: T           ) = +this * makeLiteral(other)
    operator fun <T: Any> T.           times(other: Expr<T>  ) = other.makeLiteral(this) * other
    operator fun <T: Any> T.           times(other: Column<E, T>) = other.makeLiteral(this) * other

    operator fun <T: Any> Expr<T>.  div(other: Expr<T>  ) = binary(this, BinaryOp.DIV, other)
    operator fun <T: Any> Expr<T>.  div(other: Column<E, T>) = this / +other
    operator fun <T: Any> Expr<T>.  div(other: T           ) = this / makeLiteral(other)
    operator fun <T: Any> Column<E, T>.div(other: Expr<T>  ) = +this / other
    operator fun <T: Any> Column<E, T>.div(other: Column<E, T>) = +this / +other
    operator fun <T: Any> Column<E, T>.div(other: T           ) = +this / makeLiteral(other)
    operator fun <T: Any> T.           div(other: Expr<T>  ) = other.makeLiteral(this) / other
    operator fun <T: Any> T.           div(other: Column<E, T>) = other.makeLiteral(this) / other

    operator fun <T: Any> Expr<T>.  rem(other: Expr<T>  ) = binary(this, BinaryOp.REM, other)
    operator fun <T: Any> Expr<T>.  rem(other: Column<E, T>) = this % +other
    operator fun <T: Any> Expr<T>.  rem(other: T           ) = this % makeLiteral(other)
    operator fun <T: Any> Column<E, T>.rem(other: Expr<T>  ) = +this % other
    operator fun <T: Any> Column<E, T>.rem(other: Column<E, T>) = +this % +other
    operator fun <T: Any> Column<E, T>.rem(other: T           ) = +this % makeLiteral(other)
    operator fun <T: Any> T.           rem(other: Expr<T>  ) = other.makeLiteral(this) % other
    operator fun <T: Any> T.           rem(other: Column<E, T>) = other.makeLiteral(this) % other

    fun <T: Any> coalesce(vararg options: Expr<T>, ifAllNull: T? = null): Expr<T>

    fun <T: Number> Expr<T>.orZero(): Expr<T> = coalesce(this, Literal(this.getSqlType().zeroValue, this.getSqlType()))
    fun <T: Number> NullableColumn<E, T>.orZero(): Expr<T> = coalesce(this.itself(), this.makeLiteral(this.sqlType.zeroValue))


}