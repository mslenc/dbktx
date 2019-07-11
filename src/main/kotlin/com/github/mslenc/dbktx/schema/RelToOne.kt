package com.github.mslenc.dbktx.schema

interface RelToOne<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>: RelToSingle<FROM, TO>