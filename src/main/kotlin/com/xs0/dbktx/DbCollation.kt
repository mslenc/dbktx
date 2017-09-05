package com.xs0.dbktx

import java.util.concurrent.ConcurrentHashMap

class DbCollation private constructor(val dbName: String, private val binary: Boolean, private val caseInsensitive: Boolean) {
    companion object {
        fun create(dbName: String): DbCollation {
            val binary = dbName.contains("bin")
            val caseInsensitive = !binary && dbName.contains("_ci")

            return DbCollation(dbName, binary, caseInsensitive)
        }
    }
}

object MySQLCollators {
    // we lazy create everything, because most languages will not be used most of the time..
    // TODO: we should be creating/using actual Collators for comparing strings (for keys in cache,
    // if nothing else), however I don't have enough time currently..

    private val cache = ConcurrentHashMap<String, DbCollation>()

    private const val PRIMARY: Int = 1
    private const val SECONDARY: Int = 2
    private const val TERTIARY: Int = 3

    private fun getOrCreate(name: String, languageTag: String, strength: Int = SECONDARY): DbCollation {
        // TODO: create actual collators..
        return cache.computeIfAbsent(name) { _ -> DbCollation.create(name) }
    }

    fun byName(name: String): DbCollation {
        when (name) {
            "utf8_slovenian_ci",
            "ucs2_slovenian_ci",
            "utf8mb4_slovenian_ci",
            "utf16_slovenian_ci",
            "utf32_slovenian_ci" ->
                return getOrCreate(name, "sl")

            "latin1_german1_ci",
                // TODO: return german1_ci();
            "latin1_german2_ci",
            "utf8_german2_ci",
            "ucs2_german2_ci",
            "utf8mb4_german2_ci",
            "utf16_german2_ci",
            "utf32_german2_ci" ->
                return getOrCreate(name, "de")

            "latin2_croatian_ci",
            "cp1250_croatian_ci",
            "utf8_croatian_ci",
            "ucs2_croatian_ci",
            "utf8mb4_croatian_ci",
            "utf16_croatian_ci",
            "utf32_croatian_ci" ->
                return getOrCreate(name, "hr")

            "latin2_czech_cs",
            "cp1250_czech_cs" ->
                return getOrCreate(name, "cs", TERTIARY)

            "utf8_czech_ci",
            "ucs2_czech_ci",
            "utf8mb4_czech_ci",
            "utf16_czech_ci",
            "utf32_czech_ci" ->
                return getOrCreate(name, "cs")

            "latin1_danish_ci",
            "utf8_danish_ci",
            "ucs2_danish_ci",
            "utf8mb4_danish_ci",
            "utf16_danish_ci",
            "utf32_danish_ci" ->
                return getOrCreate(name, "da")

            "utf8_esperanto_ci",
            "ucs2_esperanto_ci",
            "utf8mb4_esperanto_ci",
            "utf16_esperanto_ci",
            "utf32_esperanto_ci" ->
                return getOrCreate(name, "eo")

            "latin7_estonian_cs" ->
                return getOrCreate(name, "et", TERTIARY)

            "utf8_estonian_ci",
            "ucs2_estonian_ci",
            "utf8mb4_estonian_ci",
            "utf16_estonian_ci",
            "utf32_estonian_ci" ->
                return getOrCreate(name, "et")

            "latin2_hungarian_ci",
            "utf8_hungarian_ci",
            "ucs2_hungarian_ci",
            "utf8mb4_hungarian_ci",
            "utf16_hungarian_ci",
            "utf32_hungarian_ci" ->
                return getOrCreate(name, "hu")

            "utf8_icelandic_ci",
            "ucs2_icelandic_ci",
            "utf8mb4_icelandic_ci",
            "utf16_icelandic_ci",
            "utf32_icelandic_ci" ->
                return getOrCreate(name, "is")

            "utf8_latvian_ci",
            "ucs2_latvian_ci",
            "utf8mb4_latvian_ci",
            "utf16_latvian_ci",
            "utf32_latvian_ci" ->
                return getOrCreate(name, "lv")

            "utf8_lithuanian_ci",
            "ucs2_lithuanian_ci",
            "utf8mb4_lithuanian_ci",
            "utf16_lithuanian_ci",
            "cp1257_lithuanian_ci",
            "utf32_lithuanian_ci" ->
                return getOrCreate(name, "lt")

            "utf8_persian_ci",
            "ucs2_persian_ci",
            "utf8mb4_persian_ci",
            "utf16_persian_ci",
            "utf32_persian_ci" ->
                return getOrCreate(name, "fa")

            "cp1250_polish_ci",
            "utf8_polish_ci",
            "ucs2_polish_ci",
            "utf8mb4_polish_ci",
            "utf16_polish_ci",
            "utf32_polish_ci" ->
                return getOrCreate(name, "pl")

            "utf8_romanian_ci",
            "ucs2_romanian_ci",
            "utf8mb4_romanian_ci",
            "utf16_romanian_ci",
            "utf32_romanian_ci" ->
                return getOrCreate(name, "ro")

            "utf8_roman_ci",
            "ucs2_roman_ci",
            "utf8mb4_roman_ci",
            "utf16_roman_ci",
            "utf32_roman_ci" ->
                throw UnsupportedOperationException("Latin roman collation not supported")

            "utf8_sinhala_ci",
            "ucs2_sinhala_ci",
            "utf8mb4_sinhala_ci",
            "utf16_sinhala_ci",
            "utf32_sinhala_ci" ->
                return getOrCreate(name, "si")

            "utf8_slovak_ci",
            "ucs2_slovak_ci",
            "utf8mb4_slovak_ci",
            "utf16_slovak_ci",
            "utf32_slovak_ci" ->
                return getOrCreate(name, "sk")

            "utf8_spanish2_ci",
            "ucs2_spanish2_ci",
            "utf8mb4_spanish2_ci",
            "utf16_spanish2_ci",
            "utf32_spanish2_ci",
                // TODO: don't use the same collator for both spanish and spanish2?
            "latin1_spanish_ci",
            "utf8_spanish_ci",
            "ucs2_spanish_ci",
            "utf8mb4_spanish_ci",
            "utf16_spanish_ci",
            "utf32_spanish_ci" ->
                return getOrCreate(name, "es")

            "dec8_swedish_ci",
            "latin1_swedish_ci",
            "swe7_swedish_ci",
            "utf8_swedish_ci",
            "ucs2_swedish_ci",
            "utf8mb4_swedish_ci",
            "utf16_swedish_ci",
            "utf32_swedish_ci" ->
                return getOrCreate(name, "sv")

            "latin5_turkish_ci",
            "utf8_turkish_ci",
            "ucs2_turkish_ci",
            "utf8mb4_turkish_ci",
            "utf16_turkish_ci",
            "utf32_turkish_ci" ->
                return getOrCreate(name, "tr")

            "utf8_vietnamese_ci",
            "ucs2_vietnamese_ci",
            "utf8mb4_vietnamese_ci",
            "utf16_vietnamese_ci",
            "utf32_vietnamese_ci" ->
                return getOrCreate(name, "vi")

            "utf8_unicode_ci",
            "ucs2_unicode_ci",
            "utf8mb4_unicode_ci",
            "utf16_unicode_ci",
            "utf32_unicode_ci" ->
                return getOrCreate(name, "en-US", PRIMARY)

            "latin1_general_cs",
            "latin7_general_cs",
            "cp1251_general_cs",
                // TODO: return general_cs();

            "cp850_general_ci",
            "koi8r_general_ci",
            "latin1_general_ci",
            "latin2_general_ci",
            "ascii_general_ci",
            "hebrew_general_ci",
            "koi8u_general_ci",
            "greek_general_ci",
            "cp1250_general_ci",
            "armscii8_general_ci",
            "utf8_general_ci",
            "utf8_general_mysql500_ci",
            "ucs2_general_ci",
            "ucs2_general_mysql500_ci",
            "cp866_general_ci",
            "keybcs2_general_ci",
            "macce_general_ci",
            "macroman_general_ci",
            "cp852_general_ci",
            "latin7_general_ci",
            "utf8mb4_general_ci",
            "cp1251_general_ci",
            "utf16_general_ci",
            "utf16le_general_ci",
            "cp1256_general_ci",
            "cp1257_general_ci",
            "utf32_general_ci",
            "geostd8_general_ci" ->
                return getOrCreate(name, "en-US", PRIMARY)

            "hp8_english_ci",
            "big5_chinese_ci",
            "big5_bin",
            "dec8_bin",
            "cp850_bin",
            "hp8_bin",
            "koi8r_bin",
            "latin1_bin",
            "latin2_bin",
            "swe7_bin",
            "ascii_bin",
            "ujis_japanese_ci",
            "ujis_bin",
            "sjis_japanese_ci",
            "sjis_bin",
            "hebrew_bin",
            "tis620_thai_ci",
            "tis620_bin",
            "euckr_korean_ci",
            "euckr_bin",
            "koi8u_bin",
            "gb2312_chinese_ci",
            "gb2312_bin",
            "greek_bin",
            "cp1250_bin",
            "gbk_chinese_ci",
            "gbk_bin",
            "latin5_bin",
            "armscii8_bin",
            "utf8_bin",
            "utf8_unicode_520_ci",
            "ucs2_bin",
            "ucs2_unicode_520_ci",
            "cp866_bin",
            "keybcs2_bin",
            "macce_bin",
            "macroman_bin",
            "cp852_bin",
            "latin7_bin",
            "utf8mb4_bin",
            "utf8mb4_unicode_520_ci",
            "cp1251_bulgarian_ci",
            "cp1251_ukrainian_ci",
            "cp1251_bin",
            "utf16_bin",
            "utf16_unicode_520_ci",
            "utf16le_bin",
            "cp1256_bin",
            "cp1257_bin",
            "utf32_bin",
            "utf32_unicode_520_ci",
            "binary",
            "geostd8_bin",
            "cp932_japanese_ci",
            "cp932_bin",
            "eucjpms_japanese_ci",
            "eucjpms_bin",
            "gb18030_chinese_ci",
            "gb18030_bin",
            "gb18030_unicode_520_ci" ->
                // TODO, this is completely incorrect
                return getOrCreate(name, "en-US", PRIMARY)

            else ->
                return getOrCreate(name, "en-US", PRIMARY)
        }
    }
}
