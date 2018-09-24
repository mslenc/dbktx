package com.github.mslenc.dbktx.expr

import com.github.mslenc.dbktx.crud.TableRemapper
import com.github.mslenc.dbktx.util.Sql

class ExprDummy(val matchAll: Boolean) : ExprBoolean {
    override fun toSql(sql: Sql, topLevel: Boolean) {
        if (matchAll) {
            sql.raw("(1=1)")
        } else {
            sql.raw("(1=0)")
        }
    }

    override fun not(): ExprBoolean {
        return ExprDummy(!matchAll)
    }

    override fun remap(remapper: TableRemapper): ExprBoolean {
        return this
    }
}
