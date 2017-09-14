package com.xs0.dbktx.schemas.test1

import com.xs0.dbktx.schema.DbSchema

object TestSchema1 : DbSchema() {
    val ITEM = Item.TABLE
    val BRAND = Brand.TABLE
    val COMPANY = Company.TABLE

    init {
        finishInit()
    }
}
