package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable


class DbTicket(db: DbConn, id: Long, row: DbRow)
    : DbEntity<DbTicket, Long>(db, id, row) {

    override val metainfo get() = DbTicket

    val offerLineId: Long? get() = OFFER_LINE_ID(row)
    val personId: Long? get() = PERSON_ID(row)
    val seat: String get() = SEAT(row)

    suspend fun person() = PERSON_REF(this)
    suspend fun offerLine() = OFFER_LINE_REF(this)

    companion object : DbTable<DbTicket, Long>(TestSchema4, "ticket", DbTicket::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DbTicket::id, primaryKey = true, autoIncrement = true)

        val OFFER_LINE_ID = b.nullableLong("offer_line_id", BIGINT(), DbTicket::offerLineId)
        val PERSON_ID = b.nullableLong("person_id", BIGINT(), DbTicket::personId)
        val SEAT = b.nonNullString("seat", VARCHAR(255), DbTicket::seat)

        val OFFER_LINE_REF = b.relToOne(OFFER_LINE_ID, DbOfferLine::class)
        val PERSON_REF = b.relToOne(PERSON_ID, DbPerson::class)

        init {
            b.build(::DbTicket)
        }
    }
}
