package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

internal class FilterOr internal constructor(private val parts: List<FilterExpr>) : FilterExpr {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            sql.tuple(parts, separator = " OR ") {
                +it
            }
        }
    }

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
        internal fun create(left: FilterExpr, right: FilterExpr): FilterExpr {
            if (left == MatchNothing)
                return right

            if (right == MatchNothing)
                return left

            if (left == MatchAnything || right == MatchAnything)
                return MatchAnything

            val combined = ArrayList<FilterExpr>()

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

        internal fun create(vararg parts: FilterExpr): FilterExpr {
            val combined = ArrayList<FilterExpr>()

            for (part in parts) {
                when (part) {
                    MatchAnything -> return MatchAnything
                    MatchNothing -> { /* ignore */ }
                    is FilterOr -> combined.addAll(part.parts)
                    else -> combined += part
                }
            }

            return when {
                combined.isEmpty() -> MatchNothing
                combined.size == 1 -> combined[0]
                else -> FilterOr(combined)
            }
        }

        internal fun create(parts: Collection<FilterExpr>): FilterExpr {
            val combined = ArrayList<FilterExpr>()

            for (part in parts) {
                when (part) {
                    MatchAnything -> return MatchAnything
                    MatchNothing -> { /* ignore */ }
                    is FilterOr -> combined.addAll(part.parts)
                    else -> combined += part
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