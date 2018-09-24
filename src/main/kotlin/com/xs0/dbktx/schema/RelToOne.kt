package com.xs0.dbktx.schema

interface RelToOne<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>> {
    suspend operator fun invoke(from: FROM): TO?

    val targetTable: DbTable<TO, *>
}