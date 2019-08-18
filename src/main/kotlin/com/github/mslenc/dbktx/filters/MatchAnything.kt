package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

object MatchAnything : FilterExpr {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        sql.raw("(1=1)")
    }

    override fun not(): FilterExpr {
        return MatchNothing
    }

    override fun and(other: FilterExpr): FilterExpr {
        return other
    }

    override fun or(other: FilterExpr): FilterExpr {
        return this
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return this
    }
}