package com.github.mslenc.dbktx.schemas.test3

import com.github.mslenc.dbktx.schema.DbSchema

object TestSchema3 : DbSchema() {
    val CONTACT_INFO = ContactInfo
    val EMPLOYEE = Employee
    val INVOICE = Invoice
    val INVOICE_TIME_ITEM = InvoiceTimeItem
    val INVOICE_DAILY_TIME_ITEM = InvoiceDailyTimeItem
    val INVOICE_EXPENSE_ITEM = InvoiceExpenseItem
    val INVOICE_DAILY_EXPENSE_ITEM = InvoiceDailyExpenseItem
    val TIME_ITEM = TimeItem
    val DAILY_TIME_ITEM = DailyTimeItem
    val TASK = Task

    init {
        finishInit()
    }
}
