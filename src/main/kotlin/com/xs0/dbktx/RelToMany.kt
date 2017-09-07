package com.xs0.dbktx

interface RelToMany<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>> {
    fun contains(setFilter: ExprBoolean<TO>): ExprBoolean<FROM>
}