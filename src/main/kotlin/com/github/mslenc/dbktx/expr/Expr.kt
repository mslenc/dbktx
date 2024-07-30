package com.github.mslenc.dbktx.expr

import com.github.mslenc.asyncdb.DbType
import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.filters.FilterAnd
import com.github.mslenc.dbktx.filters.FilterNegate
import com.github.mslenc.dbktx.filters.FilterOr
import com.github.mslenc.dbktx.sqltypes.SqlType
import com.github.mslenc.dbktx.sqltypes.SqlTypeBoolean
import com.github.mslenc.dbktx.util.Sql

interface SqlEmitter {
    fun toSql(sql: Sql, nullWillBeFalse: Boolean, topLevel: Boolean)

    fun toSqlStringForDebugging(): String {
        val sql = Sql(DbType.POSTGRES)
        toSql(sql, true, true)
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

    val sqlType: SqlType<T>

    fun makeLiteral(value: T): Expr<T> = Literal(value, sqlType)

    val couldBeNull: Boolean
    val isComposite: Boolean
        get() = false

    val involvesAggregation: Boolean
}

interface AggrExpr<T: Any>: Expr<T> {
    override val involvesAggregation: Boolean
        get() = true
}

interface NullableAggrExpr<T: Any> : AggrExpr<T> {
    override val couldBeNull: Boolean
        get() = true
}

interface NonNullAggrExpr<T: Any> : AggrExpr<T> {
    override val couldBeNull: Boolean
        get() = false
}


infix fun Expr<Boolean>.and(other: Expr<Boolean>): Expr<Boolean> {
    return FilterAnd.create(this, other)
}

infix fun Expr<Boolean>.or(other: Expr<Boolean>): Expr<Boolean> {
    return FilterOr.create(this, other)
}

operator fun Expr<Boolean>.not(): Expr<Boolean> {
    return when (this) {
        is FilterExpr -> this.not()
        else -> FilterNegate(this)
    }
}

interface FilterExpr : Expr<Boolean> {
    operator fun not(): Expr<Boolean>

    infix fun and(other: Expr<Boolean>): Expr<Boolean> {
        return FilterAnd.create(this, other)
    }

    infix fun or(other: Expr<Boolean>): Expr<Boolean> {
        return FilterOr.create(this, other)
    }

    override val sqlType: SqlType<Boolean>
        get() = SqlTypeBoolean.INSTANCE_FOR_FILTER

    companion object {
        fun createOR(vararg exprs: Expr<Boolean>): Expr<Boolean> {
            return FilterOr.create(*exprs)
        }

        fun createOR(exprs: Collection<Expr<Boolean>>): Expr<Boolean> {
            return FilterOr.create(exprs)
        }

        fun createAND(vararg exprs: Expr<Boolean>): Expr<Boolean> {
            return FilterAnd.create(*exprs)
        }

        fun createAND(exprs: Collection<Expr<Boolean>>): Expr<Boolean> {
            return FilterAnd.create(exprs)
        }
    }
}

