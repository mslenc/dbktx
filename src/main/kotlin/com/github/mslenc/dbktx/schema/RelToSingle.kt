package com.github.mslenc.dbktx.schema

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.util.getContextDb

interface RelToSingle<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>: Rel<FROM, TO> {
    suspend operator fun invoke(from: FROM, db: DbConn = getContextDb()): TO?
}