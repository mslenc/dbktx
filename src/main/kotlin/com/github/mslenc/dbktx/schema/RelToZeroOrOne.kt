package com.github.mslenc.dbktx.schema

import com.github.mslenc.dbktx.util.BatchingLoader

interface RelToZeroOrOne<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>> : RelToSingle<FROM, TO>, RelOppositeOne<FROM, TO>, BatchingLoader<FROM, TO?>