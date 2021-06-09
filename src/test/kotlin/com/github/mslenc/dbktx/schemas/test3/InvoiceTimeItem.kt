package com.github.mslenc.dbktx.schemas.test3

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.BOOLEAN
import com.github.mslenc.dbktx.fieldprops.DECIMAL
import com.github.mslenc.dbktx.fieldprops.INT
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import java.math.BigDecimal

class InvoiceTimeItem(id: Long, val row: DbRow) : DbEntity<InvoiceTimeItem, Long>(id) {
    override val metainfo get() = InvoiceTimeItem

    val sortOrder: Int get() = SORT_ORDER(row)
    val invoiceId: Long get() = INVOICE_ID(row)
    val employeeId: Long get() = EMPLOYEE_ID(row)
    val hourlyRate: BigDecimal get() = HOURLY_RATE(row)
    val billable: Boolean get() = BILLABLE(row)

    companion object : DbTable<InvoiceTimeItem, Long>(TestSchema3, "invoice_time_item", InvoiceTimeItem::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), InvoiceTimeItem::id, primaryKey = true, autoIncrement = true)
        val SORT_ORDER = b.nonNullInt("sort_order", INT(), InvoiceTimeItem::sortOrder)
        val INVOICE_ID = b.nonNullLong("invoice_id", BIGINT(), InvoiceTimeItem::invoiceId)
        val EMPLOYEE_ID = b.nonNullLong("employee_id", BIGINT(), InvoiceTimeItem::employeeId)
        val HOURLY_RATE = b.nonNullDecimal("hourly_rate", DECIMAL(8, 2), InvoiceTimeItem::hourlyRate)
        val BILLABLE = b.nonNullBoolean("billable", BOOLEAN(), InvoiceTimeItem::billable)

        val INVOICE_REF = b.relToOne(INVOICE_ID, Invoice::class)
        val EMPLOYEE_REF = b.relToOne(EMPLOYEE_ID, Employee::class)

        val DAILY_ITEMS_SET = b.relToMany { InvoiceDailyTimeItem.INVOICE_TIME_ITEM_REF }

        init {
            b.build(::InvoiceTimeItem)
        }
    }
}