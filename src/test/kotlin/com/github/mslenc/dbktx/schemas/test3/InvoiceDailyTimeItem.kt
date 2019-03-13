package com.github.mslenc.dbktx.schemas.test3

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.*
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import java.math.BigDecimal
import java.time.LocalDate

class InvoiceDailyTimeItem(db: DbConn, id: Long, row: DbRow) : DbEntity<InvoiceDailyTimeItem, Long>(db, id, row) {
    override val metainfo get() = InvoiceDailyTimeItem

    val dateWorked: LocalDate get() = DATE_WORKED(row)
    val hours: BigDecimal get() = HOURS(row)
    val invoiceTimeItemId: Long get() = INVOICE_TIME_ITEM_ID(row)

    companion object : DbTable<InvoiceDailyTimeItem, Long>(TestSchema3, "invoice_daily_time_item", InvoiceDailyTimeItem::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), InvoiceDailyTimeItem::id, primaryKey = true, autoIncrement = true)
        val DATE_WORKED = b.nonNullDate("date_worked", DATE(), InvoiceDailyTimeItem::dateWorked)
        val HOURS = b.nonNullDecimal("hours", DECIMAL(4,2), InvoiceDailyTimeItem::hours)
        val INVOICE_TIME_ITEM_ID = b.nonNullLong("invoice_time_item_id", BIGINT(), InvoiceDailyTimeItem::invoiceTimeItemId)

        val INVOICE_TIME_ITEM_REF = b.relToOne(INVOICE_TIME_ITEM_ID, InvoiceTimeItem::class)

        init {
            b.build(::InvoiceDailyTimeItem)
        }
    }
}