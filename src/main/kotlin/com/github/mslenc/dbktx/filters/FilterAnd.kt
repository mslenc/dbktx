package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.expr.not
import com.github.mslenc.dbktx.util.Sql

internal class FilterAnd internal constructor(private val parts: List<Expr<Boolean>>) : FilterExpr {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            sql.tuple(parts, separator = " AND ") {
                +it
            }
        }
    }

    override val couldBeNull: Boolean
        get() = parts.any { it.couldBeNull }

    override val involvesAggregation: Boolean
        get() = parts.any { it.involvesAggregation }

    override fun not(): Expr<Boolean> {
        return FilterOr(parts.map { !it })
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterAnd(parts.map { it.remap(remapper) })
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }

    override infix fun and(other: Expr<Boolean>): FilterExpr {
        return when (other) {
            MatchAnything -> this
            MatchNothing -> MatchNothing
            is FilterAnd -> FilterAnd(parts + other.parts)
            else -> FilterAnd(parts + other)
        }
    }

    companion object {
        internal fun create(left: Expr<Boolean>, right: Expr<Boolean>): Expr<Boolean> {
            if (left == MatchAnything)
                return right

            if (right == MatchAnything)
                return left

            if (left == MatchNothing || right == MatchNothing)
                return MatchNothing

            val combined = ArrayList<Expr<Boolean>>()

            if (left is FilterAnd) {
                combined.addAll(left.parts)
            } else {
                combined.add(left)
            }
            if (right is FilterAnd) {
                combined.addAll(right.parts)
            } else {
                combined.add(right)
            }

            return FilterAnd(combined)
        }

        internal fun create(vararg parts: Expr<Boolean>): Expr<Boolean> {
            return create(parts.asList())
        }

        internal fun create(parts: Collection<Expr<Boolean>>): Expr<Boolean> {
            val combined = ArrayList<Expr<Boolean>>()

            for (part in parts) {
                when (part) {
                    MatchAnything -> { /* ignore */ }
                    MatchNothing -> return MatchNothing
                    is FilterAnd -> combined.addAll(part.parts)
                    else -> combined += part
                }
            }

            return when {
                combined.isEmpty() -> MatchAnything
                combined.size == 1 -> combined[0]
                else -> FilterAnd(combined)
            }
        }
    }
}