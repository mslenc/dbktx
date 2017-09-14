package com.xs0.dbktx.sqltypes

import org.junit.Test

import java.util.UUID

import com.xs0.dbktx.sqltypes.SqlTypeKind.*
import org.junit.Assert.*

class SqlTypeUUIDTest {
    @Test
    fun testSanity() {
        val kinds = arrayOf(CHAR, VARCHAR, BINARY, VARBINARY)
        val sizes = intArrayOf(16, 22, 24, 32, 36)

        for (kind in kinds) {
            for (size in sizes) {
                val sqlType = SqlTypeUUID.create(kind, size, true)

                val input = UUID.randomUUID()
                val output = sqlType.fromJson(sqlType.toJson(input))

                assertEquals(input, output)
            }
        }
    }

}