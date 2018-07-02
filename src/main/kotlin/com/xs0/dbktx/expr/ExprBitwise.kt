package com.xs0.dbktx.expr

import com.xs0.dbktx.crud.TableRemapper
import com.xs0.dbktx.util.Sql

internal class ExprBitwise<E, T>(private val left: Expr<in E, T>, private val op: Op, private val right: Expr<in E, T>) : ExprBoolean {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        when (op) {
            Op.HAS_ANY_BITS -> {
                sql.expr(topLevel) {
                    +left
                    raw(" & ")
                    +right
                }
            }

            Op.HAS_NO_BITS -> {
                sql.expr(topLevel) {
                    sql.paren {
                        +left
                        raw(" & ")
                        +right
                    }
                    raw(" = 0")
                }
            }
        }
    }

    override fun remap(remapper: TableRemapper): ExprBoolean {
        return ExprBitwise(left.remap(remapper), op, right.remap(remapper))
    }

    internal enum class Op {
        HAS_ANY_BITS,
        HAS_NO_BITS
    }

    override fun not(): ExprBoolean {
        return when (op) {
            Op.HAS_ANY_BITS -> ExprBitwise(left, Op.HAS_NO_BITS, right)
            Op.HAS_NO_BITS -> ExprBitwise(left, Op.HAS_ANY_BITS, right)
        }
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}
