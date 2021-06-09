package com.github.mslenc.dbktx.schemas.test1

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.DATETIME
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable

import java.time.LocalDateTime
import java.util.*

class Company(id: UUID, val row: DbRow)
    : DbEntity<Company, UUID>(id) {

    override val metainfo get() = Company

    val name: String get() = NAME(row)
    val timeCreated: LocalDateTime get() = T_CREATED(row)
    val timeUpdated: LocalDateTime get() = T_UPDATED(row)

    companion object : DbTable<Company, UUID>(TestSchema1, "companies", Company::class, UUID::class) {
        val ID = b.nonNullUUID("id", VARCHAR(36), Company::id, primaryKey = true)
        val NAME = b.nonNullString("name", VARCHAR(255), Company::name)
        val T_CREATED = b.nonNullDateTime("t_created", DATETIME(), Company::timeCreated)
        val T_UPDATED = b.nonNullDateTime("t_updated", DATETIME(), Company::timeUpdated)

        val BRANDS_SET = b.relToMany { Brand.COMPANY_REF }
        val ITEMS_SET = b.relToMany { Item.COMPANY_REF }

        val CONTACT_INFO_REF = b.relToZeroOrOne { ContactInfo.COMPANY_REF }

        init {
            b.build(::Company)
        }
    }
}
