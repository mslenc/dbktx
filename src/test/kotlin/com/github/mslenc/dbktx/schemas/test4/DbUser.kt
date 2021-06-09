package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.fieldprops.VARCHAR
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTable
import com.github.mslenc.utils.CachedAsync

class DbUser(id: Long, val row: DbRow) : DbEntity<DbUser, Long>(id) {

    override val metainfo get() = DbUser

    val userName: String get() = USER_NAME(row)
    val reportsToId: Long? get() = REPORTS_TO_ID(row)

    suspend fun reportsTo() = REPORTS_TO_REF(this)
    suspend fun roles() = ROLES_SET(this)

    private val _resolvedLevel = CachedAsync {
        var result = 0
        roles().forEach {
            val auth = it.role()
            if (auth.level > result)
                result = auth.level
        }
        result
    }
    suspend fun roleLevel(): Int {
        return _resolvedLevel.get()
    }

    companion object : DbTable<DbUser, Long>(TestSchema4, "user", DbUser::class, Long::class) {
        val ID = b.nonNullLong("id", BIGINT(), DbUser::id, primaryKey = true, autoIncrement = true)
        val USER_NAME = b.nonNullString("user_name", VARCHAR(255), DbUser::userName)
        val REPORTS_TO_ID = b.nullableLong("reports_to_id", BIGINT(), DbUser::reportsToId)

        val REPORTS_TO_REF = b.relToOne(REPORTS_TO_ID, DbUser::class)
        val ROLES_SET = b.relToMany { DbUserRole.USER_REF }

        init {
            b.build(::DbUser)
        }
    }
}