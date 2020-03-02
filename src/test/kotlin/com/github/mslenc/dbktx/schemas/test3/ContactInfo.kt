package com.github.mslenc.dbktx.schemas.test3

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable

class ContactInfo(db: DbConn, id: Long, row: DbRow) : DbEntity<ContactInfo, Long>(db, id, row) {
    override val metainfo get() = ContactInfo

    val firstName: String? get() = FIRST_NAME(row)
    val lastName: String? get() = LAST_NAME(row)
    val street1: String? get() = STREET_1(row)
    val street2: String? get() = STREET_2(row)
    val employeeId: Long? get() = EMPLOYEE_ID(row)

    companion object : DbTable<ContactInfo, Long>(TestSchema3, "contact_info_2", ContactInfo::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), ContactInfo::id, primaryKey = true, autoIncrement = true)
        val FIRST_NAME = b.nullableString("first_name", VARCHAR(255), ContactInfo::firstName)
        val LAST_NAME = b.nullableString("last_name", VARCHAR(255), ContactInfo::lastName)
        val STREET_1 = b.nullableString("street_1", VARCHAR(255), ContactInfo::street1)
        val STREET_2 = b.nullableString("street_2", VARCHAR(255), ContactInfo::street2)
        val EMPLOYEE_ID = b.nullableLong("employee_id", BIGINT(), ContactInfo::employeeId)

        val EMPLOYEE_REF = b.relToOne(EMPLOYEE_ID, Employee::class)

        init {
            b.build(::ContactInfo)
        }
    }
}