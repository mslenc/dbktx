package com.xs0.dbktx.expr

import com.xs0.dbktx.crud.TableRemapper
import com.xs0.dbktx.util.Sql

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
