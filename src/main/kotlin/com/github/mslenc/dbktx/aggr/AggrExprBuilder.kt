package com.github.mslenc.dbktx.aggr

import com.github.mslenc.dbktx.expr.BinaryOp
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.Literal
import com.github.mslenc.dbktx.schema.*

interface RelPath<BASE: DbEntity<BASE, *>, LAST: DbEntity<LAST, *>>

interface AggrExprBuilder<E: DbEntity<E, *>> {
    fun <T: Any> Column<E, T>.itself(): Expr<E, T>

    operator fun <T: Any> Column<E, T>.unaryPlus(): Expr<E, T> {
        return this.itself()
    }

    operator fun <MID: DbEntity<MID, *>, T: Any> RelToOne<E, MID>.rangeTo(column: Column<MID, T>): Expr<E, T>
    operator fun <MID: DbEntity<MID, *>, T: Any> RelToMany<E, MID>.rangeTo(column: Column<MID, T>): Expr<E, T>
    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>> RelToOne<E, MID>.rangeTo(relToOne: RelToOne<MID, NEXT>): RelPath<E, NEXT>
    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>> RelToOne<E, MID>.rangeTo(relToMany: RelToMany<MID, NEXT>): RelPath<E, NEXT>
    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>> RelToMany<E, MID>.rangeTo(relToOne: RelToOne<MID, NEXT>): RelPath<E, NEXT>
    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>> RelToMany<E, MID>.rangeTo(relToMany: RelToMany<MID, NEXT>): RelPath<E, NEXT>
    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>> RelPath<E, MID>.rangeTo(relToOne: RelToOne<MID, NEXT>): RelPath<E, NEXT>
    operator fun <MID: DbEntity<MID, *>, NEXT: DbEntity<NEXT, *>> RelPath<E, MID>.rangeTo(relToMany: RelToMany<MID, NEXT>): RelPath<E, NEXT>
    operator fun <MID: DbEntity<MID, *>, T: Any> RelPath<E, MID>.rangeTo(column: Column<MID, T>): Expr<E, T>

    fun <T: Any> binary(left: Expr<E, T>, op: BinaryOp, right: Expr<E, T>): Expr<E, T>

    operator fun <T: Any> Expr<E, T>.  plus(other: Expr<E, T>  ) = binary(this, BinaryOp.PLUS, other)
    operator fun <T: Any> Expr<E, T>.  plus(other: Column<E, T>) = this + +other
    operator fun <T: Any> Expr<E, T>.  plus(other: T           ) = this + makeLiteral(other)
    operator fun <T: Any> Column<E, T>.plus(other: Expr<E, T>  ) = +this + other
    operator fun <T: Any> Column<E, T>.plus(other: Column<E, T>) = +this + +other
    operator fun <T: Any> Column<E, T>.plus(other: T           ) = +this + makeLiteral(other)
    operator fun <T: Any> T.           plus(other: Expr<E, T>  ) = other.makeLiteral(this) + other
    operator fun <T: Any> T.           plus(other: Column<E, T>) = other.makeLiteral(this) + other

    operator fun <T: Any> Expr<E, T>.  minus(other: Expr<E, T>  ) = binary(this, BinaryOp.MINUS, other)
    operator fun <T: Any> Expr<E, T>.  minus(other: Column<E, T>) = this - +other
    operator fun <T: Any> Expr<E, T>.  minus(other: T           ) = this - makeLiteral(other)
    operator fun <T: Any> Column<E, T>.minus(other: Expr<E, T>  ) = +this - other
    operator fun <T: Any> Column<E, T>.minus(other: Column<E, T>) = +this - +other
    operator fun <T: Any> Column<E, T>.minus(other: T           ) = +this - makeLiteral(other)
    operator fun <T: Any> T.           minus(other: Expr<E, T>  ) = other.makeLiteral(this) - other
    operator fun <T: Any> T.           minus(other: Column<E, T>) = other.makeLiteral(this) - other

    operator fun <T: Any> Expr<E, T>.  times(other: Expr<E, T>  ) = binary(this, BinaryOp.TIMES, other)
    operator fun <T: Any> Expr<E, T>.  times(other: Column<E, T>) = this * +other
    operator fun <T: Any> Expr<E, T>.  times(other: T           ) = this * makeLiteral(other)
    operator fun <T: Any> Column<E, T>.times(other: Expr<E, T>  ) = +this * other
    operator fun <T: Any> Column<E, T>.times(other: Column<E, T>) = +this * +other
    operator fun <T: Any> Column<E, T>.times(other: T           ) = +this * makeLiteral(other)
    operator fun <T: Any> T.           times(other: Expr<E, T>  ) = other.makeLiteral(this) * other
    operator fun <T: Any> T.           times(other: Column<E, T>) = other.makeLiteral(this) * other

    operator fun <T: Any> Expr<E, T>.  div(other: Expr<E, T>  ) = binary(this, BinaryOp.DIV, other)
    operator fun <T: Any> Expr<E, T>.  div(other: Column<E, T>) = this / +other
    operator fun <T: Any> Expr<E, T>.  div(other: T           ) = this / makeLiteral(other)
    operator fun <T: Any> Column<E, T>.div(other: Expr<E, T>  ) = +this / other
    operator fun <T: Any> Column<E, T>.div(other: Column<E, T>) = +this / +other
    operator fun <T: Any> Column<E, T>.div(other: T           ) = +this / makeLiteral(other)
    operator fun <T: Any> T.           div(other: Expr<E, T>  ) = other.makeLiteral(this) / other
    operator fun <T: Any> T.           div(other: Column<E, T>) = other.makeLiteral(this) / other

    operator fun <T: Any> Expr<E, T>.  rem(other: Expr<E, T>  ) = binary(this, BinaryOp.REM, other)
    operator fun <T: Any> Expr<E, T>.  rem(other: Column<E, T>) = this % +other
    operator fun <T: Any> Expr<E, T>.  rem(other: T           ) = this % makeLiteral(other)
    operator fun <T: Any> Column<E, T>.rem(other: Expr<E, T>  ) = +this % other
    operator fun <T: Any> Column<E, T>.rem(other: Column<E, T>) = +this % +other
    operator fun <T: Any> Column<E, T>.rem(other: T           ) = +this % makeLiteral(other)
    operator fun <T: Any> T.           rem(other: Expr<E, T>  ) = other.makeLiteral(this) % other
    operator fun <T: Any> T.           rem(other: Column<E, T>) = other.makeLiteral(this) % other

    fun <T: Any> coalesce(vararg options: Expr<E, T>, ifAllNull: T? = null): Expr<E, T>

    fun <T: Number> Expr<E, T>.orZero(): Expr<E, T> = coalesce(this, Literal(this.getSqlType().zeroValue, this.getSqlType()))
    fun <T: Number> NullableColumn<E, T>.orZero(): Expr<E, T> = coalesce(this.itself(), this.makeLiteral(this.sqlType.zeroValue))


}