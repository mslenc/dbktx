package com.xs0.dbktx.schemas.test2

import com.xs0.dbktx.schema.DbSchema

object TestSchema2 : DbSchema() {
    val WEIGHT = Weight
    val COUNTRY = Country
    val PERSON = Person
    val COMPETITION = Competition
    val COMP_ENTRY = CompEntry
    val COMP_RESULT = CompResult

    init {
        finishInit()
    }
}
