package com.xs0.dbktx.expr

import com.xs0.dbktx.crud.TableRemapper
import com.xs0.dbktx.util.Sql

internal class ExprBools internal constructor(private val parts: List<ExprBoolean>, private val op: Op) : ExprBoolean {
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

    override fun not(): ExprBoolean {
        // !(A && B)   <=>  !A || !B
        // !(A || B)   <=>  !A && !B

        val otherOp = if (op == Op.AND) Op.OR else Op.AND

        return ExprBools(parts.map { it.not() }, otherOp)
    }

    override fun remap(remapper: TableRemapper): ExprBoolean {
        return ExprBools(parts.map { it.remap(remapper) }, op)
    }

    companion object {
        internal fun create(left: ExprBoolean, op: Op, right: ExprBoolean): ExprBools {
            val parts = ArrayList<ExprBoolean>()

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

        internal fun create(op: Op, parts: Iterable<ExprBoolean>): ExprBoolean {
            val partsList = parts.toList()

            if (partsList.isEmpty())
                throw IllegalArgumentException("Can't have an empty list of parts with OR or AND")

            if (partsList.size == 1)
                return partsList.single()

            return ExprBools(partsList, op)
        }
    }
}