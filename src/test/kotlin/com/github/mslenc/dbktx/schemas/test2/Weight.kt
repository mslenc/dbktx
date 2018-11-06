package com.github.mslenc.dbktx.schemas.test2

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.INT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable


class Weight(db: DbConn, id: Int, row: DbRow)
    : DbEntity<Weight, Int>(db, id, row) {

    override val metainfo get() = Weight

    val name:String get() = NAME(row)

    companion object : DbTable<Weight, Int>(TestSchema2, "weights", Weight::class, Int::class) {
        val ID_WEIGHT = b.nonNullInt("id_weight", INT(), Weight::id, primaryKey = true, autoIncrement = true)
        val NAME = b.nonNullString("name", VARCHAR(255), Weight::name)

        init {
            b.build(::Weight)
        }
    }
}
