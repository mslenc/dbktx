package com.github.mslenc.dbktx.schemas.test1

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.*
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import java.time.LocalDateTime
import java.util.*

class Purchase(db: DbConn, id: Long, row: DbRow)
    : DbEntity<Purchase, Long>(db, id, row) {

    override val metainfo get() = Purchase

    val companyId: UUID get() = COMPANY_ID(row)

    val timeCreated: LocalDateTime get() = T_CREATED(row)
    val timeUpdated: LocalDateTime get() = T_UPDATED(row)

    companion object : DbTable<Purchase, Long>(TestSchema1, "purchases", Purchase::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), Purchase::id, primaryKey = true, autoIncrement = true)
        val COMPANY_ID = b.nonNullUUID("company_id", VARCHAR(36), Purchase::companyId)
        val T_CREATED = b.nonNullDateTime("t_created", DATETIME(), Purchase::timeCreated)
        val T_UPDATED = b.nonNullDateTime("t_updated", DATETIME(), Purchase::timeUpdated)

        val COMPANY_REF = b.relToOne(COMPANY_ID, Company::class)

        val ITEMS_SET = b.relToMany { PurchaseItem.PURCHASE_REF }

        init {
            b.build(::Purchase)
        }
    }
}
