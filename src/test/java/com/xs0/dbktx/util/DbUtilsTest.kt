package com.xs0.dbktx.util

import org.junit.Test

import org.junit.Assert.*

class UtilsTest {
    @Test
    fun testTscapeSqlLikePattern() {
        assertEquals("", escapeSqlLikePattern("", '|'))
        assertEquals("abc", escapeSqlLikePattern("abc", '|'))
        assertEquals("abc||def||ghi||jkl", escapeSqlLikePattern("abc|def|ghi|jkl", '|'))
        assertEquals("a|%b|_c||d", escapeSqlLikePattern("a%b_c|d", '|'))
        assertEquals("a,%b,_c|d", escapeSqlLikePattern("a%b_c|d", ','))
        assertEquals("!%", escapeSqlLikePattern("%", '!'))
        assertEquals("!_", escapeSqlLikePattern("_", '!'))
        assertEquals("!!", escapeSqlLikePattern("!", '!'))
    }

    @Test
    fun testPutBytes() {
        val bytes = ByteArray(30)

        putIntBE(0xCAFE_BABE.toInt(), bytes, 1)
//        putIntLE(0xDEAD_BEEF.toInt(), bytes, 6)
        putLongBE(0x2132_4354_6576_8798L, bytes, 11)
//        putLongLE(0xA1B2_C3D4_E5F6_0718L, bytes, 20)

        assertEquals(0x00, (bytes[0].toInt() and 255))
        assertEquals(0xCA, (bytes[1].toInt() and 255))
        assertEquals(0xFE, (bytes[2].toInt() and 255))
        assertEquals(0xBA, (bytes[3].toInt() and 255))
        assertEquals(0xBE, (bytes[4].toInt() and 255))
        assertEquals(0x00, (bytes[5].toInt() and 255))
        assertEquals(0xEF, (bytes[6].toInt() and 255))
        assertEquals(0xBE, (bytes[7].toInt() and 255))
        assertEquals(0xAD, (bytes[8].toInt() and 255))
        assertEquals(0xDE, (bytes[9].toInt() and 255))
        assertEquals(0x00, (bytes[10].toInt() and 255))
        assertEquals(0x21, (bytes[11].toInt() and 255))
        assertEquals(0x32, (bytes[12].toInt() and 255))
        assertEquals(0x43, (bytes[13].toInt() and 255))
        assertEquals(0x54, (bytes[14].toInt() and 255))
        assertEquals(0x65, (bytes[15].toInt() and 255))
        assertEquals(0x76, (bytes[16].toInt() and 255))
        assertEquals(0x87, (bytes[17].toInt() and 255))
        assertEquals(0x98, (bytes[18].toInt() and 255))
        assertEquals(0x00, (bytes[19].toInt() and 255))
        assertEquals(0x18, (bytes[20].toInt() and 255))
        assertEquals(0x07, (bytes[21].toInt() and 255))
        assertEquals(0xF6, (bytes[22].toInt() and 255))
        assertEquals(0xE5, (bytes[23].toInt() and 255))
        assertEquals(0xD4, (bytes[24].toInt() and 255))
        assertEquals(0xC3, (bytes[25].toInt() and 255))
        assertEquals(0xB2, (bytes[26].toInt() and 255))
        assertEquals(0xA1, (bytes[27].toInt() and 255))
        assertEquals(0x00, (bytes[28].toInt() and 255))
        assertEquals(0x00, (bytes[29].toInt() and 255))
    }

    @Test
    fun testGetBytes() {
        val bytes = ByteArray(30)

        bytes[0] = 0x00.toByte()
        bytes[1] = 0xCA.toByte()
        bytes[2] = 0xFE.toByte()
        bytes[3] = 0xBA.toByte()
        bytes[4] = 0xBE.toByte()
        bytes[5] = 0x00.toByte()
        bytes[6] = 0xEF.toByte()
        bytes[7] = 0xBE.toByte()
        bytes[8] = 0xAD.toByte()
        bytes[9] = 0xDE.toByte()
        bytes[10] = 0x00.toByte()
        bytes[11] = 0x21.toByte()
        bytes[12] = 0x32.toByte()
        bytes[13] = 0x43.toByte()
        bytes[14] = 0x54.toByte()
        bytes[15] = 0x65.toByte()
        bytes[16] = 0x76.toByte()
        bytes[17] = 0x87.toByte()
        bytes[18] = 0x98.toByte()
        bytes[19] = 0x00.toByte()
        bytes[20] = 0x18.toByte()
        bytes[21] = 0x07.toByte()
        bytes[22] = 0xF6.toByte()
        bytes[23] = 0xE5.toByte()
        bytes[24] = 0xD4.toByte()
        bytes[25] = 0xC3.toByte()
        bytes[26] = 0xB2.toByte()
        bytes[27] = 0xA1.toByte()
        bytes[28] = 0x00.toByte()
        bytes[29] = 0x00.toByte()

        assertEquals(0xCAFE_BABE.toInt(), extractIntBE(bytes, 1))
//        assertEquals(0xDEAD_BEEF.toInt(), extractIntLE(bytes, 6))
        assertEquals(0x2132_4354_6576_8798L, extractLongBE(bytes, 11))
//        assertEquals(0xA1B2_C3D4_E5F6_0718L, extractLongLE(bytes, 20))
    }
}