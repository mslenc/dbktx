package com.github.mslenc.dbktx.schemas.test2

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.INT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable

class Country(db: DbConn, id: Int, row: DbRow)
    : DbEntity<Country, Int>(db, id, row) {

    override val metainfo get() = Country

    val name:String get() = NAME(row)

    companion object : DbTable<Country, Int>(TestSchema2, "countries", Country::class, Int::class) {
        val ID_COUNTRY = b.nonNullInt("id_country", INT(), Country::id, primaryKey = true, autoIncrement = true)
        val NAME = b.nonNullString("name", VARCHAR(255), Country::name)

        init {
            b.build(::Country)
        }
    }
}
