package com.github.mslenc.dbktx.schemas.test1

import com.github.mslenc.asyncdb.common.RowData
import com.github.mslenc.dbktx.composite.CompositeId2
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.*
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTableC
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.*

class Item(db: DbConn, id: Item.Id, private val row: RowData)
    : DbEntity<Item, Item.Id>(db, id) {

    override val metainfo get() = Item

    val sku get() = id.sku
    val companyId get() = id.company_id

    val name: String get() = NAME(row)
    val brandKey: String get() = BRAND_KEY(row)
    val price: BigDecimal get() = PRICE(row)
    val timeCreated: LocalDateTime get() = T_CREATED(row)
    val timeUpdated: LocalDateTime get() = T_UPDATED(row)

    class Id : CompositeId2<Item, UUID, String, Id> {
        override val column1 get() = COMPANY_ID
        override val column2 get() = SKU

        val company_id:UUID get() = component1
        val sku:String get() = component2

        constructor(companyId: UUID, sku: String) : super(companyId, sku)
        constructor(row: RowData) : super(row)

        override val tableMetainfo get() = Item
    }

    companion object : DbTableC<Item, Id>(TestSchema1, "items", Item::class, Id::class) {
        val COMPANY_ID = b.nonNullUUID("company_id", VARCHAR(36), Item::companyId)
        val SKU = b.nonNullString("sku", VARCHAR(255), Item::sku)
        val BRAND_KEY = b.nonNullString("brand_key", VARCHAR(255), Item::brandKey)
        val NAME = b.nonNullString("name", VARCHAR(255), Item::name)
        val PRICE = b.nonNullDecimal("price", DECIMAL(9,2), Item::price)
        val T_CREATED = b.nonNullDateTime("t_created", DATETIME(), Item::timeCreated)
        val T_UPDATED = b.nonNullDateTime("t_updated", DATETIME(), Item::timeUpdated)

        val ID = b.primaryKey(::Id)

        val BRAND_REF = b.relToOne(BRAND_KEY, COMPANY_ID, Brand::class)
        val COMPANY_REF = b.relToOne(COMPANY_ID, Company::class)

        init {
            b.build(::Item)
        }
    }
}
