package com.github.mslenc.dbktx.schemas.test2

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.INT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable


class Competition(db: DbConn, id: Int, row: DbRow)
    : DbEntity<Competition, Int>(db, id, row) {

    override val metainfo get() = Competition

    val name: String get() = NAME(row)

    companion object : DbTable<Competition, Int>(TestSchema2, "competitions", Competition::class, Int::class) {
        val ID_COMPETITION = b.nonNullInt("id_competition", INT(), Competition::id, primaryKey = true, autoIncrement = true)
        val NAME = b.nonNullString("name", VARCHAR(255), Competition::name)

        init {
            b.build(::Competition)
        }
    }
}
