package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.DECIMAL
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import java.math.BigDecimal

class DbProduct(db: DbConn, id: Long, row: DbRow)
    : DbEntity<DbProduct, Long>(db, id, row) {

    override val metainfo get() = DbProduct

    val name:String get() = NAME(row)
    val price: BigDecimal get() = CURRENT_PRICE(row)

    companion object : DbTable<DbProduct, Long>(TestSchema4, "product", DbProduct::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DbProduct::id, primaryKey = true, autoIncrement = true)
        val NAME = b.nonNullString("name", VARCHAR(255), DbProduct::name)
        val CURRENT_PRICE = b.nonNullDecimal("current_price", DECIMAL(9,2), DbProduct::price)

        init {
            b.build(::DbProduct)
        }
    }
}
