package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.DECIMAL
import com.github.mslenc.dbktx.fieldprops.INT
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import java.math.BigDecimal


class DbOfferLine(db: DbConn, id: Long, row: DbRow)
    : DbEntity<DbOfferLine, Long>(db, id, row) {

    override val metainfo get() = DbOfferLine

    val offerId: Long get() = OFFER_ID(row)
    val idx: Int get() = IDX(row)
    val productId: Long get() = PRODUCT_ID(row)
    val itemPrice: BigDecimal = ITEM_PRICE(row)

    suspend fun product() = PRODUCT_REF(this)!!
    suspend fun tickets() = TICKETS_SET(this)

    companion object : DbTable<DbOfferLine, Long>(TestSchema4, "offer_line", DbOfferLine::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DbOfferLine::id, primaryKey = true, autoIncrement = true)
        val OFFER_ID = b.nonNullLong("offer_id", BIGINT(), DbOfferLine::offerId)
        val IDX = b.nonNullInt("idx", INT(), DbOfferLine::idx)
        val PRODUCT_ID = b.nonNullLong("product_id", BIGINT(), DbOfferLine::productId)
        val ITEM_PRICE = b.nonNullDecimal("item_price", DECIMAL(9,2), DbOfferLine::itemPrice)

        val OFFER_REF = b.relToOne(OFFER_ID, DbOffer::class)
        val PRODUCT_REF = b.relToOne(PRODUCT_ID, DbProduct::class)
        val TICKETS_SET = b.relToMany { DbTicket.OFFER_LINE_REF }

        init {
            b.build(::DbOfferLine)
        }
    }
}
