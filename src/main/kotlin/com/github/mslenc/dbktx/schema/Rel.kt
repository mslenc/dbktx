package com.github.mslenc.dbktx.schema

interface Rel<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>> {
    val targetTable: DbTable<TO, *>
}