package com.xs0.dbktx.schemas.test1

import com.xs0.dbktx.schema.DbSchema

object TestSchema1 : DbSchema() {
    val ITEM = Item
    val BRAND = Brand
    val COMPANY = Company

    init {
        finishInit()
    }
}
