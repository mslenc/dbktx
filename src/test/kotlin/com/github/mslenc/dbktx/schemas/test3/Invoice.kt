package com.github.mslenc.dbktx.schemas.test3

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.DATE
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import java.time.LocalDate


class Invoice(db: DbConn, id: Long, row: DbRow) : DbEntity<Invoice, Long>(db, id, row) {
    override val metainfo get() = Invoice

    val invoiceDate: LocalDate get() = INVOICE_DATE(row)

    companion object : DbTable<Invoice, Long>(TestSchema3, "invoice", Invoice::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), Invoice::id, primaryKey = true, autoIncrement = true)
        val INVOICE_DATE = b.nonNullDate("invoice_date", DATE(), Invoice::invoiceDate)

        val TIME_ITEMS_SET = b.relToMany { InvoiceTimeItem.INVOICE_REF }

        init {
            b.build(::Invoice)
        }
    }
}