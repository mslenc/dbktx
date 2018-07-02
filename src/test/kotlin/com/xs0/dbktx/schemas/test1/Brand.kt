package com.xs0.dbktx.schemas.test1

import com.xs0.dbktx.composite.CompositeId2
import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.fieldprops.BIGINT
import com.xs0.dbktx.fieldprops.DATETIME
import com.xs0.dbktx.fieldprops.VARCHAR
import com.xs0.dbktx.schema.*
import java.time.LocalDateTime

class Brand(db: DbConn, id: Brand.Id, private val row: List<Any?>)
    : DbEntity<Brand, Brand.Id>(db, id) {

    override val metainfo get() = Brand

    val key: String get() = id.key
    val companyId: Long get() = id.companyId

    val name: String get() = NAME(row)
    val tagLine: String? get() = TAG_LINE(row)
    val timeCreated: LocalDateTime get() = T_CREATED(row)
    val timeUpdated: LocalDateTime get() = T_UPDATED(row)

    class Id : CompositeId2<Brand, String, Long, Id> {
        constructor(key: String, companyId: Long) : super(key, companyId)
        constructor(row: List<Any?>) : super(row)

        override val column1 get() = KEY
        override val column2 get() = COMPANY_ID

        val key: String get() = component1
        val companyId: Long get() = component2

        override val tableMetainfo get() = Brand
    }

    companion object : DbTableC<Brand, Brand.Id>(TestSchema1, "brands", Brand::class, Id::class) {
        val COMPANY_ID = b.nonNullLong("company_id", BIGINT(), Brand::companyId)
        val KEY = b.nonNullString("key", VARCHAR(255), Brand::key)

        val NAME = b.nonNullString("name", VARCHAR(255), Brand::name)
        val TAG_LINE = b.nullableString("tag_line", VARCHAR(255), Brand::tagLine)
        val T_CREATED = b.nonNullDateTime("t_created", DATETIME(), Brand::timeCreated)
        val T_UPDATED = b.nonNullDateTime("t_updated", DATETIME(), Brand::timeUpdated)

        val ID = b.primaryKey(::Id)

        val COMPANY_REF = b.relToOne(COMPANY_ID, Company::class)
        val ITEMS_SET = b.relToMany { Item.BRAND_REF }

        init {
            b.build(::Brand)
        }
    }
}
