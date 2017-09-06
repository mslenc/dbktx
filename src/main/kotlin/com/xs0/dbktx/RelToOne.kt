package com.xs0.dbktx

interface RelToOne<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>> {
    fun has(relatedProperty: Expr<TO, Boolean>): ExprBoolean<FROM>
}