package com.github.mslenc.dbktx.expr

import com.github.mslenc.asyncdb.DbType
import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.filters.FilterAnd
import com.github.mslenc.dbktx.filters.FilterBetween
import com.github.mslenc.dbktx.filters.FilterOr
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.util.Sql

interface SqlEmitter {
    fun toSql(sql: Sql, topLevel: Boolean = false)

    fun toSqlStringForDebugging(): String {
        val sql = Sql(DbType.POSTGRES)
        toSql(sql, true)
        return sql.getSql()
    }
}

class SqlRange<T : Any>(val minumum: Expr<T>,
                        val maximum: Expr<T>)

interface Expr<T : Any> : SqlEmitter {
    operator fun rangeTo(other: Expr<T>): SqlRange<T> {
        return SqlRange(this, other)
    }

    fun remap(remapper: TableRemapper): Expr<T>

    fun getSqlType(): SqlType<T>

    fun makeLiteral(value: T): Expr<T> = Literal(value, getSqlType())

    val couldBeNull: Boolean
    val isComposite: Boolean
        get() = false
}


interface OrderedExpr<T : Any> : Expr<T> {
    infix fun between(range: SqlRange<T>): FilterExpr {
        return FilterBetween(this, range.minumum, range.maximum, between = true)
    }

    fun between(minimum: Expr<T>, maximum: Expr<T>): FilterExpr {
        return FilterBetween(this, minimum, maximum, between = true)
    }

    infix fun notBetween(range: SqlRange<T>): FilterExpr {
        return FilterBetween(this, range.minumum, range.maximum, between = false)
    }

    fun notBetween(minimum: Expr<T>, maximum: Expr<T>): FilterExpr {
        return FilterBetween(this, minimum, maximum, between = false)
    }
}

interface ExprString : OrderedExpr<String>


operator fun Expr<Boolean>.not() {

}


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