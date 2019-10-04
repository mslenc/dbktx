package com.github.mslenc.dbktx.schema

interface RelToSingle<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>: Rel<FROM, TO> {
    suspend operator fun invoke(from: FROM): TO?
}