package com.xs0.dbktx.schema

import com.xs0.dbktx.expr.ExprBoolean

interface RelToMany<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>> {
    fun contains(setFilter: ExprBoolean<TO>): ExprBoolean<FROM>
}