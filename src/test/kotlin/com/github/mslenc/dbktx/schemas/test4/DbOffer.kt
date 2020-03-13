package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.*
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import java.time.LocalDate


class DbOffer(db: DbConn, id: Long, row: DbRow)
    : DbEntity<DbOffer, Long>(db, id, row) {

    override val metainfo get() = DbOffer

    val offerDate: LocalDate get() = OFFER_DATE(row)
    val personId: Long get() = PERSON_ID(row)

    suspend fun person() = PERSON_REF(this)!!
    suspend fun lines() = LINES_SET(this)

    companion object : DbTable<DbOffer, Long>(TestSchema4, "offer", DbOffer::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DbOffer::id, primaryKey = true, autoIncrement = true)
        val OFFER_DATE = b.nonNullDate("offer_date", DATE(), DbOffer::offerDate)
        val PERSON_ID = b.nonNullLong("person_id", BIGINT(), DbOffer::personId)

        val PERSON_REF = b.relToOne(PERSON_ID, DbPerson::class)

        val LINES_SET = b.relToMany { DbOfferLine.OFFER_REF }

        init {
            b.build(::DbOffer)
        }
    }
}
