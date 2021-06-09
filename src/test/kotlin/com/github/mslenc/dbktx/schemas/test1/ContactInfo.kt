package com.github.mslenc.dbktx.schemas.test1

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import java.util.*

class ContactInfo(id: UUID, val row: DbRow)
    : DbEntity<ContactInfo, UUID>(id) {

    override val metainfo get() = ContactInfo

    val companyId: UUID? get() = COMPANY_ID(row)
    val address: String get() = ADDRESS(row)

    companion object : DbTable<ContactInfo, UUID>(TestSchema1, "contact_info", ContactInfo::class, UUID::class) {
        val ID = b.nonNullUUID("id", VARCHAR(36), ContactInfo::id, primaryKey = true)
        val COMPANY_ID = b.nullableUUID("company_id", VARCHAR(36), ContactInfo::companyId)
        val ADDRESS = b.nonNullString("address", VARCHAR(255), ContactInfo::address)

        val COMPANY_REF = b.relToOne(COMPANY_ID, Company::class)

        init {
            b.build(::ContactInfo)
        }
    }
}
