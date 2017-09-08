package com.xs0.dbktx.schema

import com.xs0.dbktx.expr.ExprBoolean

interface RelToOne<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>> {
    fun has(relatedProperty: ExprBoolean<TO>): ExprBoolean<FROM>

    infix fun eq(ref: TO): ExprBoolean<FROM>
    infix fun oneOf(refs: Iterable<TO>): ExprBoolean<FROM>
}