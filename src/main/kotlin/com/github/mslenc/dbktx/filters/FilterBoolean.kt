package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

internal class FilterBoolean internal constructor(private val parts: List<FilterExpr>, private val op: Op) : FilterExpr {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.expr(topLevel) {
            sql.tuple(parts, separator = op.sql) {
                +it
            }
        }
    }

    internal enum class Op(internal val sql: String) {
        AND(" AND "),
        OR(" OR ")
    }

    override fun not(): FilterExpr {
        // !(A && B)   <=>  !A || !B
        // !(A || B)   <=>  !A && !B

        val otherOp = if (op == Op.AND) Op.OR else Op.AND

        return FilterBoolean(parts.map { it.not() }, otherOp)
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterBoolean(parts.map { it.remap(remapper) }, op)
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }

    companion object {
        internal fun create(left: FilterExpr, op: Op, right: FilterExpr): FilterBoolean {
            val parts = ArrayList<FilterExpr>()

            if (left is FilterBoolean && left.op == op) {
                parts.addAll(left.parts)
            } else {
                parts.add(left)
            }

            if (right is FilterBoolean && right.op == op) {
                parts.addAll(right.parts)
            } else {
                parts.add(right)
            }

            return FilterBoolean(parts, op)
        }

        internal fun create(op: Op, parts: Iterable<FilterExpr>): FilterExpr {
            val partsList = parts.toList()

            if (partsList.isEmpty())
                throw IllegalArgumentException("Can't have an empty list of parts with OR or AND")

            if (partsList.size == 1)
                return partsList.single()

            return FilterBoolean(partsList, op)
        }
    }
}