package com.xs0.dbktx.schema

import com.xs0.dbktx.crud.FilterBuilder
import com.xs0.dbktx.expr.ExprBoolean

interface RelToMany<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>> {
    val targetTable: DbTable<TO, *>

    suspend operator fun invoke(from: FROM): List<TO>
    suspend operator fun invoke(from: FROM, block: FilterBuilder<TO>.() -> ExprBoolean): List<TO>

    suspend fun countAll(from: FROM): Long
    suspend fun count(from: FROM, block: FilterBuilder<TO>.() -> ExprBoolean): Long
}