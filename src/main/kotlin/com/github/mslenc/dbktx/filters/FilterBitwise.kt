package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

internal class FilterBitwise<T: Any>(private val left: Expr<T>, private val op: Op, private val right: Expr<T>) : FilterExpr {
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

    override val couldBeNull: Boolean
        get() = left.couldBeNull || right.couldBeNull

    override val involvesAggregation: Boolean
        get() = left.involvesAggregation || right.involvesAggregation

    override fun remap(remapper: TableRemapper): FilterExpr {
        return FilterBitwise(left.remap(remapper), op, right.remap(remapper))
    }

    internal enum class Op {
        HAS_ANY_BITS,
        HAS_NO_BITS
    }

    override fun not(): FilterExpr {
        return when (op) {
            Op.HAS_ANY_BITS -> FilterBitwise(left, Op.HAS_NO_BITS, right)
            Op.HAS_NO_BITS -> FilterBitwise(left, Op.HAS_ANY_BITS, right)
        }
    }

    override fun toString(): String {
        return toSqlStringForDebugging()
    }
}
