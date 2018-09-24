package com.github.mslenc.dbktx.schemas.test2

import com.github.mslenc.dbktx.schema.DbSchema

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
