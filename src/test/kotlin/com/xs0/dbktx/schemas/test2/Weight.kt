package com.xs0.dbktx.schemas.test2

import com.xs0.asyncdb.common.RowData
import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.fieldprops.INT
import com.xs0.dbktx.fieldprops.VARCHAR
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable


class Weight(db: DbConn, id: Int, private val row: RowData)
    : DbEntity<Weight, Int>(db, id) {

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
