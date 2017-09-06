package com.xs0.dbktx

interface RelToMany<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>> {
    fun contains(setFilter: Expr<TO, Boolean>): ExprBoolean<FROM>
}