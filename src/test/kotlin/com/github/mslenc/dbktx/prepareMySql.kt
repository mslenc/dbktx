package com.github.mslenc.dbktx

import com.github.mslenc.asyncdb.DbConfig
import com.github.mslenc.asyncdb.DbTxIsolation
import com.github.mslenc.asyncdb.DbTxMode
import com.github.mslenc.asyncdb.DbType
import com.github.mslenc.asyncdb.jdbc.JdbcNonAsyncDbConnection
import com.github.mslenc.asyncdb.jdbc.JdbcSyncConnection
import com.github.mslenc.dbktx.conn.DbConn
import com.github.mslenc.dbktx.conn.DbLoaderImpl
import com.github.mslenc.dbktx.conn.SimpleRequestTime
import com.github.mslenc.dbktx.util.DebuggingConnection
import com.github.mslenc.dbktx.util.makeDbContext
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import java.sql.DriverManager
import java.time.Instant
import java.time.ZoneId

fun runMysqlTest(block: suspend (DbConn) -> Unit) {
    DriverManager.getConnection("jdbc:mysql://localhost:13306/testdb?useServerPrepStmts=true", "testUser", "testPass").use { conn ->
        for (statement in listOf(DbConfig.DEFAULT_MYSQL_INIT_SQL) + initMySql) {
            conn.prepareStatement(statement).use {
                it.execute()
            }
        }

        val config =
            DbConfig.newBuilder(DbType.MYSQL).
                setDefaultTxMode(DbTxMode.READ_WRITE).
                setDefaultTxIsolation(DbTxIsolation.REPEATABLE_READ).
            build()

        val db = DebuggingConnection(JdbcNonAsyncDbConnection(JdbcSyncConnection(config, conn) { it.close() }))
        val requestTime = SimpleRequestTime(Instant.now(), ZoneId.systemDefault())

        runBlocking {
            val loader = DbLoaderImpl(db, this, requestTime)

            val handler = CoroutineExceptionHandler { _, exception ->
                println("Caught $exception")
            }

            launch(handler + makeDbContext(loader)) {
                block(loader)
            }.join()
        }
    }
}

val initMySql = listOf(
"DROP TABLE IF EXISTS schedule_request_mapping",
"DROP TABLE IF EXISTS task",
"DROP TABLE IF EXISTS user_role",
"DROP TABLE IF EXISTS role",
"DROP TABLE IF EXISTS pay_rate",
"DROP TABLE IF EXISTS schedule_request",
"DROP TABLE IF EXISTS schedule_time",
"DROP TABLE IF EXISTS employment_profile",
"DROP TABLE IF EXISTS time_type",
"DROP TABLE IF EXISTS \"user\"",

"DROP TABLE IF EXISTS ticket",
"DROP TABLE IF EXISTS offer_line",
"DROP TABLE IF EXISTS product",
"DROP TABLE IF EXISTS offer",
"DROP TABLE IF EXISTS person",
"DROP TABLE IF EXISTS contact_info_2",
"DROP TABLE IF EXISTS employee",
"DROP TABLE IF EXISTS purchase_items",
"DROP TABLE IF EXISTS purchases",
"DROP TABLE IF EXISTS items",
"DROP TABLE IF EXISTS contact_info",
"DROP TABLE IF EXISTS brands",
"DROP TABLE IF EXISTS companies",

"""
CREATE TABLE companies(
    id VARCHAR(36) NOT NULL PRIMARY KEY COLLATE utf8_bin,
    name VARCHAR(255) NOT NULL,
    t_created DATETIME NOT NULL,
    t_updated DATETIME NOT NULL
) engine=InnoDB
""",

"""
CREATE TABLE brands(
    company_id VARCHAR(36) NOT NULL COLLATE utf8_bin,
    "key" VARCHAR(255) NOT NULL,

    name VARCHAR(255) NOT NULL,
    tag_line VARCHAR(255) NULL,
    t_created DATETIME NOT NULL,
    t_updated DATETIME NOT NULL,

    PRIMARY KEY(company_id, "key"),
    FOREIGN KEY(company_id) REFERENCES companies(id)
) engine=InnoDB
""",

"""
CREATE TABLE contact_info(
    id VARCHAR(36) NOT NULL COLLATE utf8_bin,
    company_id VARCHAR(36) NULL COLLATE utf8_bin,
    address VARCHAR(255) NOT NULL,
    PRIMARY KEY(id),
    FOREIGN KEY(company_id) REFERENCES companies(id)
) engine=InnoDB
""",

"""
CREATE TABLE items(
company_id VARCHAR(36) NOT NULL COLLATE utf8_bin,
sku VARCHAR(255) NOT NULL,
brand_key VARCHAR(255) NOT NULL,
name VARCHAR(255) NOT NULL,
price DECIMAL(9,2) NOT NULL,
t_created DATETIME NOT NULL,
t_updated DATETIME NOT NULL,
PRIMARY KEY(company_id, sku),
FOREIGN KEY(company_id) REFERENCES companies(id),
FOREIGN KEY(company_id, brand_key) REFERENCES brands(company_id, "key")
)
""",

"""
CREATE TABLE purchases(
id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
company_id VARCHAR(36) NOT NULL COLLATE utf8_bin,
t_created DATETIME NOT NULL,
t_updated DATETIME NOT NULL,

FOREIGN KEY(company_id) REFERENCES companies(id)
);
""",

"""
CREATE TABLE purchase_items(
id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
company_id VARCHAR(36) NOT NULL COLLATE utf8_bin,
sku VARCHAR(255) NOT NULL,
purchase_id BIGINT NOT NULL,

price DECIMAL(9,2) NOT NULL,
t_created DATETIME NOT NULL,
t_updated DATETIME NOT NULL,

UNIQUE KEY(id, company_id, sku),
FOREIGN KEY(company_id, sku) REFERENCES items(company_id, sku),
FOREIGN KEY(purchase_id) REFERENCES purchases(id)
)
""",

"""
CREATE TABLE employee (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL
)
""",

"""
CREATE TABLE contact_info_2 (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(255) NULL,
    last_name VARCHAR(255) NULL,
    street_1 VARCHAR(255) NULL,
    street_2 VARCHAR(255) NULL,
    employee_id BIGINT NULL,
    
    FOREIGN KEY(employee_id) REFERENCES employee(id)
)
""",

"""
CREATE TABLE person (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL
)
""",

"""
CREATE TABLE offer (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    offer_date DATE NOT NULL,
    person_id BIGINT NOT NULL,
    FOREIGN KEY(person_id) REFERENCES person(id)
)
""",

"""
CREATE TABLE product (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    current_price DECIMAL(9,2) NOT NULL
)
""",

"""
CREATE TABLE offer_line (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    offer_id BIGINT NOT NULL,
    idx INT NOT NULL,
    product_id BIGINT NOT NULL,
    item_price DECIMAL(9,2) NOT NULL,
    
    FOREIGN KEY(offer_id) REFERENCES offer(id),
    FOREIGN KEY(product_id) REFERENCES product(id)
)
""",

"""
CREATE TABLE ticket (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    offer_line_id BIGINT NULL,
    person_id BIGINT NULL,
    seat VARCHAR(255) NOT NULL,
    FOREIGN KEY(offer_line_id) REFERENCES offer_line(id),
    FOREIGN KEY(person_id) REFERENCES person(id)
)
""",

"""
CREATE TABLE "user" (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_name VARCHAR(255) NOT NULL,
    reports_to_id BIGINT NULL,

    FOREIGN KEY(reports_to_id) REFERENCES "user"(id)
)
""",

"""
CREATE TABLE "time_type" (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
)
""",

"""
CREATE TABLE "employment_profile" (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
)
""",

"""
CREATE TABLE schedule_time (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    time_type_id BIGINT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES "user"(id),
    FOREIGN KEY(time_type_id) REFERENCES time_type(id)
)
""",

"""
CREATE TABLE schedule_request (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    schedule_time_id BIGINT NOT NULL,
    first_date DATE NOT NULL,
    comment TEXT NULL,
    FOREIGN KEY (schedule_time_id) REFERENCES schedule_time(id)
)
""",

"""
CREATE TABLE pay_rate (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    date_effective DATE NOT NULL,
    hourly_rate DECIMAL(9, 2) NOT NULL,
    profile_id BIGINT NOT NULL,

    FOREIGN KEY(user_id) REFERENCES "user"(id),
    FOREIGN KEY(profile_id) REFERENCES employment_profile(id)
)
""",

"""
CREATE TABLE "role" (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    level INT NOT NULL
)
""",

"""
CREATE TABLE "user_role" (
    user_id BIGINT NOT NULL,
    role_id BIGINT NOT NULL,
    PRIMARY KEY(user_id, role_id),
    FOREIGN KEY(user_id) REFERENCES "user"(id),
    FOREIGN KEY(role_id) REFERENCES "role"(id)
)
""",

"""
CREATE TABLE task (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    approver_id BIGINT NULL,

    FOREIGN KEY(approver_id) REFERENCES "user"(id)
)
""",

"""
CREATE TABLE schedule_request_mapping (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    
    time_type_id BIGINT NOT NULL,
    employment_profile_id BIGINT NOT NULL,
    internal_task_id BIGINT NOT NULL,
    
    FOREIGN KEY(time_type_id) REFERENCES time_type(id),
    FOREIGN KEY(employment_profile_id) REFERENCES employment_profile(id),
    FOREIGN KEY(internal_task_id) REFERENCES task(id)      
)
""",

"""
INSERT INTO employee(id, first_name, last_name) VALUES 
    (1, 'Johnny', 'Smith'),
    (2, 'Mary', 'Johnson')
""",

"""
INSERT INTO contact_info_2(id, first_name, last_name, street_1, street_2, employee_id) VALUES
    (100, 'John', 'Smith', null, null, 1)
""",

"""
INSERT INTO person(id, first_name, last_name) VALUES
    (1, 'Jeremiah', 'Prime')
""",

"""
INSERT INTO offer(id, offer_date, person_id) VALUES
    (300, '2020-03-02', 1),
    (301, '2020-03-02', 1)
""",

"""
INSERT INTO product(id, name, current_price) VALUES
    (505, 'Regular Ticket', 120)
""",

"""
INSERT INTO offer_line(id, offer_id, idx, product_id, item_price) VALUES
    (1250, 300, 1, 505, 120),
    (1251, 300, 2, 505, 120),
    (1252, 301, 1, 505, 120)
""",

"""
INSERT INTO ticket(id, offer_line_id, person_id, seat) VALUES
    (3000, 1250, 1, 'A20'),
    (3001, 1251, 1, 'A21'),
    (3002, 1252, 1, 'A22')
""",




"""
INSERT INTO "user"(id, user_name, reports_to_id) VALUES
    (3, 'jboss', null),
    (17, 'msmith', 3)
""",

"""
INSERT INTO "role"(id, name, level) VALUES
    (9901, 'Admin', 500),
    (9902, 'User', 5)
""",

"""
INSERT INTO "user_role"(user_id, role_id) VALUES
    (3, 9901),
    (17, 9902)
""",

"""
INSERT INTO employment_profile(id, name) VALUES
    (22, 'Regular'),
    (33, 'Hourly')
""",

"""
INSERT INTO time_type(id, name) VALUES
    (7701, 'Working'),
    (7702, 'Vacation')
""",

"""
INSERT INTO task(id, name, approver_id) VALUES
    (8801, 'Main work', 17),
    (8802, 'Vacation work', 17)
""",

"""
INSERT INTO pay_rate(id, user_id, date_effective, hourly_rate, profile_id) VALUES
    (6601, 3, '2019-01-01', 15, 22),
    (6602, 3, '2020-01-01', 16, 22),
    (6603, 17, '2020-01-01', 11, 33)
""",

"""
INSERT INTO schedule_request_mapping(id, time_type_id, employment_profile_id, internal_task_id) VALUES
    (9901, 7701, 22, 8801),
    (9902, 7701, 33, 8802),
    (9903, 7702, 22, 8801),
    (9904, 7702, 33, 8802)
""",

"""
INSERT INTO schedule_time(id, user_id, time_type_id) VALUES
    (4421, 17, 7702)
""",

"""
INSERT INTO schedule_request(id, schedule_time_id, first_date, comment) VALUES
    (1825, 4421, '2020-03-12', null)
"""
)
