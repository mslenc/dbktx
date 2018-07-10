package com.xs0.dbktx.sqltypes

import org.junit.Test

import java.util.UUID

import com.xs0.dbktx.sqltypes.SqlTypeKind.*
import com.xs0.dbktx.util.SpecialValues.emptyUUID
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

    @Test
    fun testSanityEmptyUUID() {
        val kinds = arrayOf(CHAR, VARCHAR, BINARY, VARBINARY)
        val sizes = intArrayOf(16, 22, 24, 32, 36)

        for (kind in kinds) {
            for (size in sizes) {
                val sqlType = SqlTypeUUID.create(kind, size, true)

                val input = emptyUUID
                val output = sqlType.fromJson(sqlType.toJson(input))

                assertSame(input, output)
            }
        }
    }

    @Test
    fun testSanityEmptyUUIDNotTheOne() {
        val kinds = arrayOf(CHAR, VARCHAR, BINARY, VARBINARY)
        val sizes = intArrayOf(16, 22, 24, 32, 36)

        for (kind in kinds) {
            for (size in sizes) {
                val sqlType = SqlTypeUUID.create(kind, size, true)

                val input = UUID(0L, 0L)
                val output = sqlType.fromJson(sqlType.toJson(input))

                assertEquals(input, output)
                assertNotSame(output, emptyUUID)
            }
        }
    }

}