package com.xs0.dbktx.schemas.test2

import com.xs0.asyncdb.common.RowData
import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.fieldprops.INT
import com.xs0.dbktx.fieldprops.VARCHAR
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable


class Competition(db: DbConn, id: Int, private val row: RowData)
    : DbEntity<Competition, Int>(db, id) {

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
