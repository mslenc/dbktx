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

        repeat (1000) {
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

    @Test
    fun testSanity2() {
        // this tests against a particular bug that occurred - if the UUID started with "20" in a 16-byte
        // encoding, it would get trimmed in some (buggy) implementation, then fail to decode due to being
        // too short. So, we specifically test against that, to prevent reoccurrence..
        val kinds = arrayOf(CHAR, VARCHAR, BINARY, VARBINARY)
        val sizes = intArrayOf(16, 22, 24, 32, 36)

        for (kind in kinds) {
            for (size in sizes) {
                val sqlType = SqlTypeUUID.create(kind, size, true)

                val input = UUID.fromString("2064b9e5-0344-4c5b-a1e1-3225e72a39d1")
                val encoded = sqlType.toJson(input)
                val output = try {
                    sqlType.fromJson(encoded)
                } catch (e: ArrayIndexOutOfBoundsException) {
                    throw AssertionError("Failed with AIOOBE for $input ($kind, $size)")
                }

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