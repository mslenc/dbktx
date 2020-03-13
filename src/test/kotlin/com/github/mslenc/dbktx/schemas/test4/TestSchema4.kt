package com.github.mslenc.dbktx.schemas.test4

import com.github.mslenc.dbktx.schema.*

object TestSchema4 : DbSchema() {
    val PERSON = DbPerson
    val OFFER = DbOffer
    val PRODUCT = DbProduct
    val OFFER_LINE = DbOfferLine
    val TICKET = DbTicket

    val EMPLOYMENT_PROFILE = DbEmploymentProfile
    val PAY_RATE = DbPayRate
    val ROLE = DbRole
    val SCHEDULE_REQUEST = DbScheduleRequest
    val SCHEDULE_REQUEST_MAPPING = DbScheduleRequestMapping
    val SCHEDULE_TIME = DbScheduleTime
    val TASK = DbTask
    val TIME_TYPE = DbTimeType
    val USER = DbUser
    val USER_ROLE = DbUserRole

    init {
        finishInit()
    }
}
