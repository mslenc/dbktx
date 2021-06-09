package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.asyncdb.DbRow
import com.github.mslenc.dbktx.composite.CompositeId2
import com.github.mslenc.dbktx.fieldprops.BIGINT
import com.github.mslenc.dbktx.schema.DbEntity
import com.github.mslenc.dbktx.schema.DbTableC

class DbUserRole(id: Id, val row: DbRow) : DbEntity<DbUserRole, DbUserRole.Id>(id) {

    override val metainfo get() = DbUserRole

    val userId: Long get() = id.userId
    val roleId: Long get() = id.roleId

    suspend fun user() = USER_REF(this)!!
    suspend fun role() = ROLE_REF(this)!!

    class Id : CompositeId2<DbUserRole, Long, Long, Id> {
        constructor(userId: Long, roleId: Long) : super(userId, roleId)
        constructor(row: DbRow) : super(row)

        override val column1 get() = USER_ID
        override val column2 get() = ROLE_ID

        val userId: Long get() = component1
        val roleId: Long get() = component2

        override val tableMetainfo get() = DbUserRole
    }


    companion object : DbTableC<DbUserRole, Id>(TestSchema4, "user_role", DbUserRole::class, Id::class) {
        val USER_ID = b.nonNullLong("user_id", BIGINT(), { it.id.userId })
        val ROLE_ID = b.nonNullLong("role_id", BIGINT(), { it.id.roleId })

        val ID = b.primaryKey(::Id)

        val USER_REF = b.relToOne(USER_ID, DbUser::class)
        val ROLE_REF = b.relToOne(ROLE_ID, DbRole::class)

        init {
            b.build(::DbUserRole)
        }
    }
}