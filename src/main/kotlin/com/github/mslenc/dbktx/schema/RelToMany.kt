package com.github.mslenc.dbktx.schema

import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.expr.Expr
import com.github.mslenc.dbktx.expr.ExprBuilder
import com.github.mslenc.dbktx.util.BatchingLoader
import com.github.mslenc.dbktx.util.getContextDb

interface RelToMany<FROM : DbEntity<FROM, *>, TO : DbEntity<TO, *>>: RelOppositeOne<FROM, TO>, BatchingLoader<FROM, List<TO>> {
    suspend operator fun invoke(from: FROM, db: DbConn = getContextDb()): List<TO>
    suspend operator fun invoke(from: FROM, block: ExprBuilder<TO>.()->Expr<Boolean>, db: DbConn = getContextDb()): List<TO>

    suspend fun countAll(from: FROM, db: DbConn = getContextDb()): Long
    suspend fun count(from: FROM, block: ExprBuilder<TO>.()->Expr<Boolean>, db: DbConn = getContextDb()): Long
}