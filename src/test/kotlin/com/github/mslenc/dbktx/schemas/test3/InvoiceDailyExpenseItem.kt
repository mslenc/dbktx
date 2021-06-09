package com.github.mslenc.dbktx.schemas.test3

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.*
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import java.math.BigDecimal
import java.time.LocalDate

class InvoiceDailyExpenseItem(id: Long, val row: DbRow) : DbEntity<InvoiceDailyExpenseItem, Long>(id) {
    override val metainfo get() = InvoiceDailyExpenseItem

    val dateIncurred: LocalDate get() = DATE_INCURRED(row)
    val invoiceExpenseItemId: Long get() = INVOICE_EXPENSE_ITEM_ID(row)
    val amount: BigDecimal get() = AMOUNT(row)
    val markupAmount: BigDecimal? get() = MARKUP_AMOUNT(row)
    val markupPerc: BigDecimal? get() = MARKUP_PERC(row)

    companion object : DbTable<InvoiceDailyExpenseItem, Long>(TestSchema3, "invoice_daily_expense_item", InvoiceDailyExpenseItem::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), InvoiceDailyExpenseItem::id, primaryKey = true, autoIncrement = true)
        val DATE_INCURRED = b.nonNullDate("date_worked", DATE(), InvoiceDailyExpenseItem::dateIncurred)
        val AMOUNT = b.nonNullDecimal("amount", DECIMAL(8, 2), InvoiceDailyExpenseItem::amount)
        val MARKUP_AMOUNT = b.nullableDecimal("markup_amount", DECIMAL(8, 2), InvoiceDailyExpenseItem::markupAmount)
        val MARKUP_PERC = b.nullableDecimal("markup_perc", DECIMAL(7, 2), InvoiceDailyExpenseItem::markupPerc)
        val INVOICE_EXPENSE_ITEM_ID = b.nonNullLong("invoice_expense_item_id", BIGINT(), InvoiceDailyExpenseItem::invoiceExpenseItemId)

        val INVOICE_EXPENSE_ITEM_REF = b.relToOne(INVOICE_EXPENSE_ITEM_ID, InvoiceExpenseItem::class)

        init {
            b.build(::InvoiceDailyExpenseItem)
        }
    }
}