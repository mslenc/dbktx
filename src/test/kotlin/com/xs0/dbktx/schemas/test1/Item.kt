package com.xs0.dbktx.schemas.test1

import com.xs0.dbktx.composite.CompositeId2
import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.fieldprops.BIGINT
import com.xs0.dbktx.fieldprops.DATETIME
import com.xs0.dbktx.fieldprops.DECIMAL
import com.xs0.dbktx.fieldprops.VARCHAR
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTableC
import java.math.BigDecimal
import java.time.LocalDateTime

class Item(db: DbConn, id: Item.Id, private val row: List<Any?>)
    : DbEntity<Item, Item.Id>(db, id) {

    override val metainfo get() = TABLE

    val sku get() = id.sku
    val companyId get() = id.company_id

    val name: String get() = NAME(row)
    val brandKey: String get() = BRAND_KEY(row)
    val price: BigDecimal get() = PRICE(row)
    val timeCreated: LocalDateTime get() = T_CREATED(row)
    val timeUpdated: LocalDateTime get() = T_UPDATED(row)

    class Id : CompositeId2<Item, Long, String, Id> {
        override val column1 get() = COMPANY_ID
        override val column2 get() = SKU

        val company_id:Long get() = component1
        val sku:String get() = component2

        constructor(companyId: Long, sku: String) : super(companyId, sku)
        constructor(row: List<Any?>) : super(row)

        override val tableMetainfo get() = TABLE
    }

    companion object TABLE: DbTableC<Item, Id>(TestSchema1, "items", Item::class, Id::class) {
        val COMPANY_ID = b.nonNullLong("company_id", BIGINT(), Item::companyId)
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
