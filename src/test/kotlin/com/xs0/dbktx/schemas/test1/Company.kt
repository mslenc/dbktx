package com.xs0.dbktx.schemas.test1

import com.xs0.dbktx.conn.DbConn
import com.xs0.dbktx.fieldprops.BIGINT
import com.xs0.dbktx.fieldprops.DATETIME
import com.xs0.dbktx.fieldprops.VARCHAR
import com.xs0.dbktx.schema.DbEntity
import com.xs0.dbktx.schema.DbTable

import java.time.LocalDateTime

class Company(db: DbConn, id: Long, private val row: List<Any?>)
    : DbEntity<Company, Long>(db, id) {

    override val metainfo get() = TABLE

    val name: String get() = NAME(row)
    val timeCreated: LocalDateTime get() = T_CREATED(row)
    val timeUpdated: LocalDateTime get() = T_UPDATED(row)

    companion object TABLE: DbTable<Company, Long>(TestSchema1, "companies", Company::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), Company::id, primaryKey = true, autoIncrement = true)
        val NAME = b.nonNullString("name", VARCHAR(255), Company::name)
        val T_CREATED = b.nonNullDateTime("t_created", DATETIME(), Company::timeCreated)
        val T_UPDATED = b.nonNullDateTime("t_updated", DATETIME(), Company::timeUpdated)

        val BRANDS_SET = b.relToMany { Brand.COMPANY_REF }
        val ITEMS_SET = b.relToMany { Item.COMPANY_REF }

        init {
            b.build(::Company)
        }
    }
}
