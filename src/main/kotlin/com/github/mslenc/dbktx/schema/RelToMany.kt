package com.github.mslenc.dbktx.schema

import com.github.mslenc.dbktx.crud.FilterBuilder
import com.github.mslenc.dbktx.expr.FilterExpr
import com.github.mslenc.dbktx.util.BatchingLoader

interface RelToMany<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>: BatchingLoader<FROM, List<TO>> {
    val targetTable: DbTable<TO, *>

    suspend operator fun invoke(from: FROM): List<TO>
    suspend operator fun invoke(from: FROM, block: FilterBuilder<TO>.() -> FilterExpr): List<TO>

    suspend fun countAll(from: FROM): Long
    suspend fun count(from: FROM, block: FilterBuilder<TO>.() -> FilterExpr): Long
}