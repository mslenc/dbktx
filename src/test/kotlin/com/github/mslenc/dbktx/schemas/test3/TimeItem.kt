package com.github.mslenc.dbktx.schemas.test3

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.BOOLEAN
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable

class TimeItem(id: Long, val row: DbRow) : DbEntity<TimeItem, Long>(id) {
    override val metainfo get() = TimeItem

    val taskId: Long get() = TASK_ID(row)
    val employeeId: Long get() = EMPLOYEE_ID(row)
    val billable: Boolean get() = BILLABLE(row)

    companion object : DbTable<TimeItem, Long>(TestSchema3, "time_item", TimeItem::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), TimeItem::id, primaryKey = true, autoIncrement = true)
        val TASK_ID = b.nonNullLong("task_id", BIGINT(), TimeItem::taskId)
        val EMPLOYEE_ID = b.nonNullLong("employee_id", BIGINT(), TimeItem::employeeId)
        val BILLABLE = b.nonNullBoolean("billable", BOOLEAN(), TimeItem::billable)

        val TASK_REF = b.relToOne(TASK_ID, Task::class)
        val EMPLOYEE_REF = b.relToOne(EMPLOYEE_ID, Employee::class)

        val DAILY_ITEMS_SET = b.relToMany { DailyTimeItem.TIME_ITEM_REF }

        init {
            b.build(::TimeItem)
        }
    }
}