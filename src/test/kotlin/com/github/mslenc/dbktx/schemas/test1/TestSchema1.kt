package com.github.mslenc.dbktx.schemas.test1

import com.github.mslenc.dbktx.schema.DbSchema

object TestSchema1 : DbSchema() {
    val ITEM = Item
    val BRAND = Brand
    val COMPANY = Company
    val PURCHASE = Purchase
    val PURCHASE_ITEM = PurchaseItem

    init {
        finishInit()
    }
}
