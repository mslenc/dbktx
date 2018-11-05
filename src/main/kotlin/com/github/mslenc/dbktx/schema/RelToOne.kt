package com.github.mslenc.dbktx.schema

interface RelToOne<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>> {
    suspend operator fun invoke(from: FROM): TO?

    val targetTable: DbTable<TO, *>
}