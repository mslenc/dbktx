package com.github.mslenc.dbktx.filters

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.Sql

class FilterDummy(val matchAll: Boolean) : FilterExpr {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        if (matchAll) {
            sql.raw("(1=1)")
        } else {
            sql.raw("(1=0)")
        }
    }

    override fun not(): FilterExpr {
        return FilterDummy(!matchAll)
    }

    override fun remap(remapper: TableRemapper): FilterExpr {
        return this
    }
}
