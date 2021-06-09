package com.github.mslenc.dbktx.schemas.test3

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.*
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import java.math.BigDecimal
import java.time.LocalDate

class DailyTimeItem(id: Long, val row: DbRow) : DbEntity<DailyTimeItem, Long>(id) {
    override val metainfo get() = DailyTimeItem

    val dateWorked: LocalDate get() = DATE_WORKED(row)
    val hours: BigDecimal get() = HOURS(row)
    val nbHours: BigDecimal get() = HOURS(row)
    val timeItemId: Long get() = TIME_ITEM_ID(row)

    companion object : DbTable<DailyTimeItem, Long>(TestSchema3, "daily_time_item", DailyTimeItem::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DailyTimeItem::id, primaryKey = true, autoIncrement = true)
        val DATE_WORKED = b.nonNullDate("date_worked", DATE(), DailyTimeItem::dateWorked)
        val HOURS = b.nonNullDecimal("hours", DECIMAL(4,2), DailyTimeItem::hours)
        val NB_HOURS = b.nonNullDecimal("nb_hours", DECIMAL(4,2), DailyTimeItem::nbHours)
        val TIME_ITEM_ID = b.nonNullLong("time_item_id", BIGINT(), DailyTimeItem::timeItemId)

        val TIME_ITEM_REF = b.relToOne(TIME_ITEM_ID, TimeItem::class)

        init {
            b.build(::DailyTimeItem)
        }
    }
}