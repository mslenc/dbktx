package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

object MatchAnything : FilterExpr {
    override fun toSql(sql: Sql, nullWillBeFalse: Boolean, topLevel: Boolean) {
        sql.raw("(1=1)")
    }

    override val couldBeNull: Boolean
        get() = false

    override val involvesAggregation: Boolean
        get() = false

    override fun not(): FilterExpr {
        return MatchNothing
    }

    override fun and(other: Expr<Boolean>): Expr<Boolean> {
        return other
    }

    override fun or(other: Expr<Boolean>): FilterExpr {
        return this
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return this
    }
}