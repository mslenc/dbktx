package com.github.mslenc.dbktx.schemas.test3

import com.github.mslenc.dbktx.schema.DbSchema

object TestSchema3 : DbSchema() {
    val EMPLOYEE = Employee
    val INVOICE = Invoice
    val INVOICE_TIME_ITEM = InvoiceTimeItem
    val INVOICE_DAILY_TIME_ITEM = InvoiceDailyTimeItem

    init {
        finishInit()
    }
}
