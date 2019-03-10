package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.filters.FilterBoolean
import com.github.mslenc.dbktx.filters.FilterBetween
import com.github.mslenc.dbktx.filters.FilterIsNull
import com.github.mslenc.dbktx.util.Sql

interface SqlEmitter {
    fun toSql(sql: Sql, topLevel: Boolean = false)

    fun toSqlStringForDebugging(): String {
        val sql = Sql()
        toSql(sql, true)
        return sql.getSql()
    }
}

class SqlRange<E, T>(val minumum: Expr<in E, T>,
                     val maximum: Expr<in E, T>)

interface Expr<E, T> : SqlEmitter {
    operator fun rangeTo(other: Expr<in E, T>): SqlRange<E, T> {
        return SqlRange(this, other)
    }

    fun remap(remapper: TableRemapper): Expr<E, T>

    val isComposite: Boolean
        get() = false
}

interface NullableExpr<E, T> : Expr<E, T> {
    val isNull: FilterExpr
        get() = FilterIsNull(this, isNull = true)

    val isNotNull: FilterExpr
        get() = FilterIsNull(this, isNull = false)
}

interface NonNullExpr<E, T> : Expr<E, T>



interface OrderedExpr<E, T> : Expr<E, T> {


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

interface NullableOrderedExpr<E, T>: OrderedExpr<E, T>, NullableExpr<E, T>
interface NonNullOrderedExpr<E, T>: OrderedExpr<E, T>, NonNullExpr<E, T>


interface ExprString<E> : OrderedExpr<E, String>
interface NullableExprString<E>: ExprString<E>, NullableExpr<E, String>
interface NonNullExprString<E>: ExprString<E>, NonNullExpr<E, String>


interface FilterExpr : SqlEmitter {
    operator fun not(): FilterExpr
    fun remap(remapper: TableRemapper): FilterExpr

    companion object {
        fun createOR(exprs: Iterable<FilterExpr>): FilterExpr {
            return FilterBoolean.create(FilterBoolean.Op.OR, exprs)
        }

        fun createAND(exprs: Iterable<FilterExpr>): FilterExpr {
            return FilterBoolean.create(FilterBoolean.Op.AND, exprs)
        }
    }
}