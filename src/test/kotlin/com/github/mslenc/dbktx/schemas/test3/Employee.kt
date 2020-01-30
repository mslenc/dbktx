package com.github.mslenc.dbktx.schemas.test3

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable


class Employee(db: DbConn, id: Long, row: DbRow) : DbEntity<Employee, Long>(db, id, row) {
    override val metainfo get() = Employee

    val firstName: String get() = FIRST_NAME(row)
    val lastName: String get() = LAST_NAME(row)

    suspend fun contactInfo(): ContactInfo? = CONTACT_REF(this)

    suspend fun contactFirstName() = contactInfo()?.firstName
    suspend fun contactLastName() = contactInfo()?.lastName

    companion object : DbTable<Employee, Long>(TestSchema3, "employee", Employee::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), Employee::id, primaryKey = true, autoIncrement = true)
        val FIRST_NAME = b.nonNullString("first_name", VARCHAR(255), Employee::firstName)
        val LAST_NAME = b.nonNullString("last_name", VARCHAR(255), Employee::lastName)

        val CONTACT_REF = b.relToZeroOrOne { ContactInfo.EMPLOYEE_REF }

        init {
            b.build(::Employee)
        }
    }
}