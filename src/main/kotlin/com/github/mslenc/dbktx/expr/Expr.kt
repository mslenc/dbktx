package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.filters.FilterAnd
import com.github.mslenc.dbktx.filters.FilterBetween
import com.github.mslenc.dbktx.filters.FilterIsNull
import com.github.mslenc.dbktx.filters.FilterOr
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql

interface SqlEmitter {
    fun toSql(sql: Sql, topLevel: Boolean = false)

    fun toSqlStringForDebugging(): String {
        val sql = Sql()
        toSql(sql, true)
        return sql.getSql()
    }
}

class SqlRange<E, T : Any>(val minumum: Expr<in E, T>,
                           val maximum: Expr<in E, T>)

interface Expr<E, T : Any> : SqlEmitter {
    operator fun rangeTo(other: Expr<in E, T>): SqlRange<E, T> {
        return SqlRange(this, other)
    }

    fun remap(remapper: TableRemapper): Expr<E, T>

    fun getSqlType(): SqlType<T>

    fun makeLiteral(value: T): Expr<E, T> = Literal(value, getSqlType())

    val isComposite: Boolean
        get() = false
}

interface NullableExpr<E, T : Any> : Expr<E, T> {
    fun isNull(): FilterExpr {
        return FilterIsNull(this, isNull = true)
    }

    fun isNotNull(): FilterExpr{
        return FilterIsNull(this, isNull = false)
    }
}

interface NonNullExpr<E, T : Any> : Expr<E, T>



interface OrderedExpr<E, T : Any> : Expr<E, T> {
    infix fun between(range: SqlRange<in E, T>): FilterExpr {
        return FilterBetween(this, range.minumum, range.maximum, between = true)
    }

    fun between(minimum: Expr<in E, T>, maximum: Expr<in E, T>): FilterExpr {
        return FilterBetween(this, minimum, maximum, between = true)
    }

    infix fun notBetween(range: SqlRange<in E, T>): FilterExpr {
        return FilterBetween(this, range.minumum, range.maximum, between = false)
    }

    fun notBetween(minimum: Expr<in E, T>, maximum: Expr<in E, T>): FilterExpr {
        return FilterBetween(this, minimum, maximum, between = false)
    }
}

interface NullableOrderedExpr<E, T : Any>: OrderedExpr<E, T>, NullableExpr<E, T>
interface NonNullOrderedExpr<E, T : Any>: OrderedExpr<E, T>, NonNullExpr<E, T>


interface ExprString<E> : OrderedExpr<E, String>
interface NullableExprString<E>: ExprString<E>, NullableExpr<E, String>
interface NonNullExprString<E>: ExprString<E>, NonNullExpr<E, String>


interface FilterExpr : SqlEmitter {
    operator fun not(): FilterExpr
    fun remap(remapper: TableRemapper): FilterExpr

    infix fun and(other: FilterExpr): FilterExpr {
        return FilterAnd.create(this, other)
    }

    infix fun or(other: FilterExpr): FilterExpr {
        return FilterOr.create(this, other)
    }

    companion object {
        fun createOR(vararg exprs: FilterExpr): FilterExpr {
            return FilterOr.create(*exprs)
        }

        fun createOR(exprs: Collection<FilterExpr>): FilterExpr {
            return FilterOr.create(exprs)
        }

        fun createAND(vararg exprs: FilterExpr): FilterExpr {
            return FilterAnd.create(*exprs)
        }

        fun createAND(exprs: Collection<FilterExpr>): FilterExpr {
            return FilterAnd.create(exprs)
        }
    }
}