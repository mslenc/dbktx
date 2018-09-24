package com.xs0.dbktx.util

import java.util.*

object SpecialValues {
    /**
     * This is a special value which represents a missing UUID in the form of an empty string.
     * Note that it has to be this exact object (=== reference equality), not just a UUID full
     * of zeros.
     */
    val emptyUUID = UUID(0, 0)
}