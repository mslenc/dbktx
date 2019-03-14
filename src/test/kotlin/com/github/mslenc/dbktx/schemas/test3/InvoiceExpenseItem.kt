package com.github.mslenc.dbktx.schemas.test3

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.BOOLEAN
import com.github.mslenc.dbktx.fieldprops.DECIMAL
import com.github.mslenc.dbktx.fieldprops.INT
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import java.math.BigDecimal

class InvoiceExpenseItem(db: DbConn, id: Long, row: DbRow) : DbEntity<InvoiceExpenseItem, Long>(db, id, row) {
    override val metainfo get() = InvoiceExpenseItem

    val sortOrder: Int get() = SORT_ORDER(row)
    val invoiceId: Long get() = INVOICE_ID(row)
    val employeeId: Long get() = EMPLOYEE_ID(row)
    val billable: Boolean get() = BILLABLE(row)

    companion object : DbTable<InvoiceExpenseItem, Long>(TestSchema3, "invoice_expense_item", InvoiceExpenseItem::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), InvoiceExpenseItem::id, primaryKey = true, autoIncrement = true)
        val SORT_ORDER = b.nonNullInt("sort_order", INT(), InvoiceExpenseItem::sortOrder)
        val INVOICE_ID = b.nonNullLong("invoice_id", BIGINT(), InvoiceExpenseItem::invoiceId)
        val EMPLOYEE_ID = b.nonNullLong("employee_id", BIGINT(), InvoiceExpenseItem::employeeId)
        val BILLABLE = b.nonNullBoolean("billable", BOOLEAN(), InvoiceExpenseItem::billable)

        val INVOICE_REF = b.relToOne(INVOICE_ID, Invoice::class)
        val EMPLOYEE_REF = b.relToOne(EMPLOYEE_ID, Employee::class)

        val DAILY_ITEMS_SET = b.relToMany { InvoiceDailyExpenseItem.INVOICE_EXPENSE_ITEM_REF }

        init {
            b.build(::InvoiceExpenseItem)
        }
    }
}