package com.xs0.dbktx.expr

import com.xs0.dbktx.util.Sql

class ExprBools<E> internal constructor(private val parts: List<ExprBoolean<E>>, private val op: Op) : ExprBoolean<E> {
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

    companion object {
        internal fun <E> create(left: ExprBoolean<E>, op: Op, right: ExprBoolean<E>): ExprBools<E> {
            val parts = ArrayList<ExprBoolean<E>>()

            if (left is ExprBools && left.op == op) {
                parts.addAll(left.parts)
            } else {
                parts.add(left)
            }

            if (right is ExprBools && right.op == op) {
                parts.addAll(right.parts)
            } else {
                parts.add(right)
            }

            return ExprBools(parts, op)
        }

        internal fun <E> create(op: Op, parts: Iterable<ExprBoolean<E>>): ExprBoolean<E> {
            val partsList = parts.toList()

            if (partsList.isEmpty())
                throw IllegalArgumentException("Can't have an empty list of parts with OR or AND")

            if (partsList.size == 1)
                return partsList.single()

            return ExprBools(partsList, op)
        }
    }
}