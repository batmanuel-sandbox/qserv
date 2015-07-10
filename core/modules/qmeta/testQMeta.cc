/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "QMetaMysql.h"

// System headers
#include <iostream>
#include <fstream>
#include <string>
#include <unistd.h> // for getpass

// Third-party headers
#include  "boost/algorithm/string/replace.hpp"

// Qserv headers
#include "sql/SqlConnection.h"
#include "sql/SqlErrorObject.h"

// Local headers
#include "Exceptions.h"

// Boost unit test header
#define BOOST_TEST_MODULE QMeta_1
#include "boost/test/included/unit_test.hpp"

using lsst::qserv::mysql::MySqlConfig;
using namespace lsst::qserv::qmeta;
using lsst::qserv::sql::SqlConnection;
using lsst::qserv::sql::SqlErrorObject;

namespace {

struct TestDBGuard {
    TestDBGuard() {
        sqlConfig.hostname = "";
        sqlConfig.port = 0;
        sqlConfig.username = "root";
        sqlConfig.password = getpass("Enter mysql root password: ");
        std::cout << "Enter mysql socket: ";
        std::cin >> sqlConfig.socket;
        sqlConfig.dbName = "testQMetaZ012sdrt";

        std::ifstream schemaFile("admin/templates/configuration/tmp/configure/sql/QueryMetadata.sql");

        // read whole file into buffer
        std::string buffer;
        std::getline(schemaFile, buffer, '\0');

        // replace production schema name with test schema
        boost::replace_all(buffer, "qservMeta", sqlConfig.dbName);

        // need config without database name
        MySqlConfig sqlConfigLocal = sqlConfig;
        sqlConfigLocal.dbName = "";
        SqlConnection sqlConn(sqlConfigLocal);

        SqlErrorObject errObj;
        sqlConn.runQuery(buffer, errObj);
        if (errObj.isSet()) {
            throw SqlError(ERR_LOC, errObj);
        }
    }

    ~TestDBGuard() {
        SqlConnection sqlConn(sqlConfig);
        SqlErrorObject errObj;
        sqlConn.dropDb(sqlConfig.dbName, errObj);
    }

    MySqlConfig sqlConfig;
};

}

struct PerTestFixture {
    PerTestFixture() {
        qMeta = std::make_shared<QMetaMysql>(testDB.sqlConfig);
        sqlConn = std::make_shared<SqlConnection>(testDB.sqlConfig);
    }

    static TestDBGuard testDB;
    std::shared_ptr<SqlConnection> sqlConn;
    std::shared_ptr<QMeta> qMeta;
};

TestDBGuard PerTestFixture::testDB;


BOOST_FIXTURE_TEST_SUITE(SqlConnectionTestSuite, PerTestFixture)

BOOST_AUTO_TEST_CASE(messWithCzars) {

    // check for few non-existing names
    BOOST_CHECK_EQUAL(qMeta->getCzarID(""), -1);
    BOOST_CHECK_EQUAL(qMeta->getCzarID("unknown"), -1);

    // start with registering couple of czars
    int cid1 = qMeta->registerCzar("czar:1000");
    BOOST_CHECK_EQUAL(qMeta->getCzarID("czar:1000"), cid1);
    int cid2 = qMeta->registerCzar("czar-2:1000");
    BOOST_CHECK_EQUAL(qMeta->getCzarID("czar-2:1000"), cid2);

    // re-register existing czar, should get the same id
    int cid3 = qMeta->registerCzar("czar-2:1000");
    BOOST_CHECK_EQUAL(cid3, cid2);
    BOOST_CHECK_EQUAL(qMeta->getCzarID("czar-2:1000"), cid3);

    // activate/deactivate, check exceptions
    BOOST_CHECK_NO_THROW(qMeta->setCzarActive(cid1, false));
    BOOST_CHECK_NO_THROW(qMeta->setCzarActive(cid1, true));
    BOOST_CHECK_THROW(qMeta->setCzarActive(9999999, true), CzarIdError);
}

BOOST_AUTO_TEST_CASE(messWithQueries) {

    // make sure that we have czars from previous test
    int cid1 = qMeta->getCzarID("czar:1000");
    BOOST_CHECK(cid1 != -1);

    // resister one query
    QInfo qinfo(QInfo::INTERACTIVE, cid1, "user1", "SELECT * from Object", "SELECT * from Object_{}", "");
    QMeta::TableNames tables(1, std::make_pair("TestDB", "Object"));
    int qid1 = qMeta->registerQuery(qinfo, tables);
    BOOST_CHECK(qid1 != -1);

    // get query info
    QInfo qinfo1 = qMeta->getQueryInfo(qid1);
    BOOST_CHECK_EQUAL(qinfo1.queryType(), qinfo.queryType());
    BOOST_CHECK_EQUAL(qinfo1.czarId(), qinfo.czarId());
    BOOST_CHECK_EQUAL(qinfo1.user(), qinfo.user());
    BOOST_CHECK_EQUAL(qinfo1.queryText(), qinfo.queryText());
    BOOST_CHECK_EQUAL(qinfo1.queryTemplate(), qinfo.queryTemplate());
    BOOST_CHECK_EQUAL(qinfo1.resultQuery(), qinfo.resultQuery());
    BOOST_CHECK(qinfo1.submitted() != std::time_t(0));
    BOOST_CHECK_EQUAL(qinfo1.collected(), std::time_t(0));
    BOOST_CHECK_EQUAL(qinfo1.completed(), std::time_t(0));
    BOOST_CHECK_EQUAL(qinfo1.duration(), std::time_t(0));

    // get running queries
    std::vector<int> queries = qMeta->getExecutingQueries(cid1);
    BOOST_CHECK_EQUAL(queries.size(), 1U);
    BOOST_CHECK_EQUAL(queries[0], qid1);

    // update collected status
    BOOST_CHECK_THROW(qMeta->markQueryCollected(99999), QueryIdError);
    qMeta->markQueryCollected(qid1);

    qinfo1 = qMeta->getQueryInfo(qid1);
    BOOST_CHECK(qinfo1.submitted() != std::time_t(0));
    BOOST_CHECK(qinfo1.collected() != std::time_t(0));
    BOOST_CHECK_EQUAL(qinfo1.completed(), std::time_t(0));
    BOOST_CHECK_EQUAL(qinfo1.duration(), std::time_t(0));

    // update completed status
    BOOST_CHECK_THROW(qMeta->finishQuery(99999), QueryIdError);
    qMeta->finishQuery(qid1);

    qinfo1 = qMeta->getQueryInfo(qid1);
    BOOST_CHECK(qinfo1.submitted() != std::time_t(0));
    BOOST_CHECK(qinfo1.collected() != std::time_t(0));
    BOOST_CHECK(qinfo1.completed() != std::time_t(0));
    BOOST_CHECK(qinfo1.duration() >= std::time_t(0));

    // no running queries should be there
    queries = qMeta->getExecutingQueries(cid1);
    BOOST_CHECK_EQUAL(queries.size(), 0U);
}

BOOST_AUTO_TEST_CASE(messWithQueries2) {

    // make sure that we have czars from previous test
    int cid1 = qMeta->getCzarID("czar:1000");
    BOOST_CHECK(cid1 != -1);
    int cid2 = qMeta->getCzarID("czar-2:1000");
    BOOST_CHECK(cid2 != -1);

    // resister few queries
    QInfo qinfo(QInfo::INTERACTIVE, cid1, "user1", "SELECT * from Object", "SELECT * from Object_{}", "");
    QMeta::TableNames tables(1, std::make_pair("TestDB", "Object"));
    int qid1 = qMeta->registerQuery(qinfo, tables);
    int qid2 = qMeta->registerQuery(qinfo, tables);
    qinfo = QInfo(QInfo::LONG_RUNNING, cid2, "user2", "SELECT * from Object", "SELECT * from Object_{}", "");
    int qid3 = qMeta->registerQuery(qinfo, tables);
    int qid4 = qMeta->registerQuery(qinfo, tables);

    // get running queries
    std::vector<int> queries = qMeta->getExecutingQueries(cid1);
    BOOST_CHECK_EQUAL(queries.size(), 2U);
    queries = qMeta->getExecutingQueries(cid2);
    BOOST_CHECK_EQUAL(queries.size(), 2U);

    // update completed status
    qMeta->finishQuery(qid1);
    qMeta->finishQuery(qid3);
    queries = qMeta->getExecutingQueries(cid1);
    BOOST_CHECK_EQUAL(queries.size(), 1U);
    queries = qMeta->getExecutingQueries(cid2);
    BOOST_CHECK_EQUAL(queries.size(), 1U);

    qMeta->finishQuery(qid2);
    qMeta->finishQuery(qid4);

    // no running queries should be there
    queries = qMeta->getExecutingQueries(cid1);
    BOOST_CHECK_EQUAL(queries.size(), 0U);
    queries = qMeta->getExecutingQueries(cid2);
    BOOST_CHECK_EQUAL(queries.size(), 0U);
}

BOOST_AUTO_TEST_CASE(messWithTables) {

    // make sure that we have czars from previous test
    int cid1 = qMeta->getCzarID("czar:1000");
    BOOST_CHECK(cid1 != -1);
    int cid2 = qMeta->getCzarID("czar-2:1000");
    BOOST_CHECK(cid2 != -1);

    // resister few queries
    QInfo qinfo(QInfo::INTERACTIVE, cid1, "user1", "SELECT * from Object", "SELECT * from Object_{}", "");
    QMeta::TableNames tables(1, std::make_pair("TestDB", "Object"));
    int qid1 = qMeta->registerQuery(qinfo, tables);
    int qid2 = qMeta->registerQuery(qinfo, tables);
    qinfo = QInfo(QInfo::LONG_RUNNING, cid2, "user2", "SELECT * from Object", "SELECT * from Object_{}", "");
    tables.push_back(std::make_pair("TestDB", "Source"));
    int qid3 = qMeta->registerQuery(qinfo, tables);
    int qid4 = qMeta->registerQuery(qinfo, tables);

    // get queries for tables
    std::vector<int> queries = qMeta->getQueriesForTable("TestDB", "Object");
    BOOST_CHECK_EQUAL(queries.size(), 4U);
    queries = qMeta->getQueriesForTable("TestDB", "Source");
    BOOST_CHECK_EQUAL(queries.size(), 2U);

    // update completed status
    qMeta->finishQuery(qid1);
    qMeta->finishQuery(qid3);
    queries = qMeta->getQueriesForTable("TestDB", "Object");
    BOOST_CHECK_EQUAL(queries.size(), 2U);
    queries = qMeta->getQueriesForTable("TestDB", "Source");
    BOOST_CHECK_EQUAL(queries.size(), 1U);

    qMeta->finishQuery(qid2);
    qMeta->finishQuery(qid4);

    // no running queries should be there
    queries = qMeta->getQueriesForTable("TestDB", "Object");
    BOOST_CHECK_EQUAL(queries.size(), 0U);
    queries = qMeta->getQueriesForTable("TestDB", "Source");
    BOOST_CHECK_EQUAL(queries.size(), 0U);
}

BOOST_AUTO_TEST_CASE(messWithChunks) {

    // make sure that we have czars from previous test
    int cid1 = qMeta->getCzarID("czar:1000");
    BOOST_CHECK(cid1 != -1);
    int cid2 = qMeta->getCzarID("czar-2:1000");
    BOOST_CHECK(cid2 != -1);

    // resister one query
    QInfo qinfo(QInfo::INTERACTIVE, cid1, "user1", "SELECT * from Object", "SELECT * from Object_{}", "");
    QMeta::TableNames tables;
    tables.push_back(std::make_pair("TestDB", "Object"));
    int qid1 = qMeta->registerQuery(qinfo, tables);
    BOOST_CHECK(qid1 != -1);

    // register few chunks and assign them to workers
    std::vector<int> chunks;
    chunks.push_back(10);
    chunks.push_back(20);
    chunks.push_back(37);
    qMeta->addChunks(qid1, chunks);

    // assign chunks to workers
    qMeta->assignChunk(qid1, 10, "worker1");
    qMeta->assignChunk(qid1, 20, "worker2");
    qMeta->assignChunk(qid1, 37, "worker2");
    BOOST_CHECK_THROW(qMeta->assignChunk(qid1, 42, "worker2"), ChunkIdError);
    BOOST_CHECK_THROW(qMeta->assignChunk(99999, 10, "worker2"), ChunkIdError);

    // re-assign chunk
    qMeta->assignChunk(qid1, 37, "worker33");

    // mark chunks as complete
    qMeta->finishChunk(qid1, 10);
    qMeta->finishChunk(qid1, 20);
    qMeta->finishChunk(qid1, 37);
    BOOST_CHECK_THROW(qMeta->finishChunk(qid1, 42), ChunkIdError);
}

BOOST_AUTO_TEST_SUITE_END()