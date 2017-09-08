package com.xs0.dbktx.schema

import com.xs0.dbktx.expr.Expr
import com.xs0.dbktx.expr.ExprBoolean

interface RelToOne<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>> {
    fun has(relatedProperty: Expr<TO, Boolean>): ExprBoolean<FROM>
}