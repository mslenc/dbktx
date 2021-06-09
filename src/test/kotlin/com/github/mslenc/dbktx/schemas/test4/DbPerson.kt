package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.*
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable


class DbPerson(id: Long, val row: DbRow) : DbEntity<DbPerson, Long>(id) {

    override val metainfo get() = DbPerson

    val firstName: String get() = FIRST_NAME(row)
    val lastName: String get() = LAST_NAME(row)

    suspend fun offers() = OFFERS_SET(this)
    suspend fun tickets() = TICKETS_SET(this)

    companion object : DbTable<DbPerson, Long>(TestSchema4, "person", DbPerson::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DbPerson::id, primaryKey = true, autoIncrement = true)
        val FIRST_NAME = b.nonNullString("first_name", VARCHAR(255), DbPerson::firstName)
        val LAST_NAME = b.nonNullString("last_name", VARCHAR(255), DbPerson::lastName)

        val OFFERS_SET = b.relToMany { DbOffer.PERSON_REF }
        val TICKETS_SET = b.relToMany { DbTicket.PERSON_REF }

        init {
            b.build(::DbPerson)
        }
    }
}
