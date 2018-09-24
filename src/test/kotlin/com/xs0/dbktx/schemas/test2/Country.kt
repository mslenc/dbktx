package com.xs0.dbktx.schemas.test2

import com.github.mslenc.asyncdb.common.RowData
import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.fieldprops.INT
import com.xs0.dbktx.fieldprops.VARCHAR
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable

class Country(db: DbConn, id: Int, private val row: RowData)
    : DbEntity<Country, Int>(db, id) {

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
