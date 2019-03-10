package com.github.mslenc.dbktx.schemas.test1

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.composite.CompositeId3
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.DATETIME
import com.github.mslenc.dbktx.fieldprops.DECIMAL
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.*

class PurchaseItem(db: DbConn, id: Long, row: DbRow)
    : DbEntity<PurchaseItem, Long>(db, id, row) {

    override val metainfo get() = PurchaseItem

    val companyId: UUID get() = COMPANY_ID(row)
    val sku: String get() = SKU(row)
    val price: BigDecimal get() = PRICE(row)
    val timeCreated: LocalDateTime get() = T_CREATED(row)
    val timeUpdated: LocalDateTime get() = T_UPDATED(row)
    val purchaseId: Long get() = PURCHASE_ID(row)

    val uniqueItemKey: UniqueItemKey get() = UniqueItemKey(row)

    class UniqueItemKey : CompositeId3<PurchaseItem, Long, UUID, String, UniqueItemKey> {
        override val column1 get() = ID
        override val column2 get() = COMPANY_ID
        override val column3 get() = SKU

        val id: Long get() = component1
        val companyId: UUID get() = component2
        val sku: String get() = component3

        constructor(id: Long, companyId: UUID, sku: String) : super(id, companyId, sku)
        constructor(row: DbRow) : super(row)

        override val tableMetainfo get() = PurchaseItem
    }

    companion object : DbTable<PurchaseItem, Long>(TestSchema1, "purchase_items", PurchaseItem::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), PurchaseItem::id, primaryKey = true, autoIncrement = true)
        val COMPANY_ID = b.nonNullUUID("company_id", VARCHAR(36), PurchaseItem::companyId)
        val SKU = b.nonNullString("sku", VARCHAR(255), PurchaseItem::sku)
        val PURCHASE_ID = b.nonNullLong("purchase_id", BIGINT(), PurchaseItem::purchaseId)

        val PRICE = b.nonNullDecimal("price", DECIMAL(9,2), PurchaseItem::price)
        val T_CREATED = b.nonNullDateTime("t_created", DATETIME(), PurchaseItem::timeCreated)
        val T_UPDATED = b.nonNullDateTime("t_updated", DATETIME(), PurchaseItem::timeUpdated)

        val PURCHASE_REF = b.relToOne(PURCHASE_ID, Purchase::class)
        val COMPANY_REF = b.relToOne(COMPANY_ID, Company::class)
        val ITEM_REF = b.relToOne(COMPANY_ID, SKU, Item::class, Item::Id)

        val UNIQUE_ITEM_KEY = b.uniqueKey(::UniqueItemKey, PurchaseItem::uniqueItemKey)

        init {
            b.build(::PurchaseItem)
        }
    }
}