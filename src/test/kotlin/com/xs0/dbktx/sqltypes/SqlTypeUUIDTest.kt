package com.xs0.dbktx.sqltypes

import org.junit.Test

import java.util.UUID

import com.xs0.dbktx.sqltypes.SqlTypeKind.*
import com.xs0.dbktx.util.SpecialValues.emptyUUID
import org.junit.Assert.*

class SqlTypeUUIDTest {
    @Test
    fun testSanity() {
        val types = arrayOf(
            SqlTypeUUID.create(CHAR, 32, true),
            SqlTypeUUID.create(CHAR, 36, true),
            SqlTypeUUID.create(VARCHAR, 32, true),
            SqlTypeUUID.create(VARCHAR, 36, true),
            SqlTypeUUID.create(BINARY, 16, true),
            SqlTypeUUID.create(BINARY, 36, true),
            SqlTypeUUID.create(VARBINARY, 16, true),
            SqlTypeUUID.create(VARBINARY, 36, true)
        )

        repeat (1000) {
            for (sqlType in types) {
                val input = UUID.randomUUID()
                val output = sqlType.decodeFromJson(sqlType.encodeForJson(input))

                assertEquals(input, output)
            }
        }
    }


    @Test
    fun testSanityEmptyUUID() {
        val types = arrayOf(
            SqlTypeUUID.create(CHAR, 32, true),
            SqlTypeUUID.create(CHAR, 36, true),
            SqlTypeUUID.create(VARCHAR, 32, true),
            SqlTypeUUID.create(VARCHAR, 36, true),
            SqlTypeUUID.create(BINARY, 16, true),
            SqlTypeUUID.create(BINARY, 36, true),
            SqlTypeUUID.create(VARBINARY, 16, true),
            SqlTypeUUID.create(VARBINARY, 36, true)
        )

        for (sqlType in types) {
            val input = emptyUUID
            val output = sqlType.decodeFromJson(sqlType.encodeForJson(input))

            assertSame(input, output)
        }
    }

    @Test
    fun testSanityEmptyUUIDNotTheOne() {
        val charTypes = arrayOf(
            SqlTypeUUID.create(CHAR, 32, true),
            SqlTypeUUID.create(CHAR, 36, true),
            SqlTypeUUID.create(VARCHAR, 32, true),
            SqlTypeUUID.create(VARCHAR, 36, true)
        )

        val binaryTypes = arrayOf(
            SqlTypeUUID.create(BINARY, 16, true),
            SqlTypeUUID.create(BINARY, 36, true),
            SqlTypeUUID.create(VARBINARY, 16, true),
            SqlTypeUUID.create(VARBINARY, 36, true)
        )

        for (sqlType in charTypes) {
            val input = UUID(0L, 0L)
            val output = sqlType.decodeFromJson(sqlType.encodeForJson(input))

            assertEquals(input, output)
            assertNotSame(emptyUUID, output)

            val bubu = sqlType.decodeFromJson("")
            assertSame(emptyUUID, bubu)

            val baba = sqlType.parseRowDataValue("")
            assertSame(emptyUUID, baba)
        }

        for (sqlType in binaryTypes) {
            val input = UUID(0L, 0L)
            val output = sqlType.decodeFromJson(sqlType.encodeForJson(input))

            assertEquals(input, output)
            assertNotSame(emptyUUID, output)

            val bubu = sqlType.decodeFromJson("")
            assertSame(emptyUUID, bubu)

            val baba = sqlType.parseRowDataValue(byteArrayOf())
            assertSame(emptyUUID, baba)
        }
    }

}