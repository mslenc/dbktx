package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable

class DbEmploymentProfile(db: DbConn, id: Long, row: DbRow)
    : DbEntity<DbEmploymentProfile, Long>(db, id, row) {

    override val metainfo get() = DbEmploymentProfile

    val name: String get() = NAME(row)

    companion object : DbTable<DbEmploymentProfile, Long>(TestSchema4, "employment_profile", DbEmploymentProfile::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DbEmploymentProfile::id, primaryKey = true, autoIncrement = true)
        val NAME = b.nonNullString("name", VARCHAR(255), DbEmploymentProfile::name)

        init {
            b.build(::DbEmploymentProfile)
        }
    }
}