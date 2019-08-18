package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

internal class FilterAnd internal constructor(private val parts: List<FilterExpr>) : FilterExpr {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            sql.tuple(parts, separator = " AND ") {
                +it
            }
        }
    }

    override fun not(): FilterExpr {
        return FilterOr(parts.map { !it })
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterAnd(parts.map { it.remap(remapper) })
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }

    override infix fun and(other: FilterExpr): FilterExpr {
        return when (other) {
            MatchAnything -> this
            MatchNothing -> MatchNothing
            is FilterAnd -> FilterAnd(parts + other.parts)
            else -> FilterAnd(parts + other)
        }
    }

    companion object {
        internal fun create(left: FilterExpr, right: FilterExpr): FilterExpr {
            if (left == MatchAnything)
                return right

            if (right == MatchAnything)
                return left

            if (left == MatchNothing || right == MatchNothing)
                return MatchNothing

            val combined = ArrayList<FilterExpr>()

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

        internal fun create(vararg parts: FilterExpr): FilterExpr {
            val combined = ArrayList<FilterExpr>()

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

        internal fun create(parts: Collection<FilterExpr>): FilterExpr {
            val combined = ArrayList<FilterExpr>()

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