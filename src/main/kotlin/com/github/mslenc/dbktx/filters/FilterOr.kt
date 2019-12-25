package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.expr.not
import com.github.mslenc.dbktx.util.Sql

internal class FilterOr internal constructor(private val parts: List<Expr<Boolean>>) : FilterExpr {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            sql.tuple(parts, separator = " OR ") {
                +it
            }
        }
    }

    override val couldBeNull: Boolean
        get() = parts.any { it.couldBeNull }

    override val involvesAggregation: Boolean
        get() = parts.any { it.involvesAggregation }

    override fun not(): FilterExpr {
        return FilterAnd(parts.map { !it })
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterOr(parts.map { it.remap(remapper) })
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }

    override infix fun or(other: FilterExpr): FilterExpr {
        return when (other) {
            MatchAnything -> MatchAnything
            MatchNothing -> this
            is FilterOr -> FilterOr(parts + other.parts)
            else -> FilterOr(parts + other)
        }
    }

    companion object {
        internal fun create(left: Expr<Boolean>, right: Expr<Boolean>): Expr<Boolean> {
            if (left == MatchNothing)
                return right

            if (right == MatchNothing)
                return left

            if (left == MatchAnything || right == MatchAnything)
                return MatchAnything

            val combined = ArrayList<Expr<Boolean>>()

            if (left is FilterOr) {
                combined.addAll(left.parts)
            } else {
                combined.add(left)
            }
            if (right is FilterOr) {
                combined.addAll(right.parts)
            } else {
                combined.add(right)
            }

            return FilterOr(combined)
        }

        internal fun create(vararg parts: Expr<Boolean>): Expr<Boolean> {
            return create(parts.asList())
        }

        internal fun create(parts: Collection<Expr<Boolean>>): Expr<Boolean> {
            val combined = ArrayList<Expr<Boolean>>()

            for (part in parts) {
                when (part) {
                    MatchAnything -> return MatchAnything
                    MatchNothing -> { /* ignore */ }
                    is FilterOr -> combined.addAll(part.parts)
                    else -> combined.add(part)
                }
            }

            return when {
                combined.isEmpty() -> MatchNothing
                combined.size == 1 -> combined[0]
                else -> FilterOr(combined)
            }
        }
    }
}