// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 AURA/LSST.
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
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */


// Class header
#include <boost/asio.hpp>
#include <iostream>

// Third-party headers


// qserv headers
#include "loader/LoaderMsg.h"
#include "proto/ProtoImporter.h"
#include "proto/loader.pb.h"


// LSST headers
#include "lsst/log/Log.h"
#include "Central.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.Central");
}

namespace lsst {
namespace qserv {
namespace loader {


Central::~Central() {
    _loop = false;
    _pool->shutdownPool();
    for (std::thread& thd : _ioServiceThreads) {
        thd.join();
    }
    _workerList.reset();
}


void Central::run() {
    std::thread thd([this]() { _ioService.run(); });
    _ioServiceThreads.push_back(std::move(thd));
}


void Central::_checkDoList() {
    while(_loop) {
        // Run and then sleep for a second. A more advanced timer should be used
        LOGS(_log, LOG_LVL_INFO, "\n\n &&& checking doList");
        _doList.checkList();
        sleep(1);
    }
}


void CentralWorker::monitorWorkers() {
    // Add _workerList to _doList so it starts checking new entries.
    _doList.addItem(_workerList);
}


void CentralWorker::registerWithMaster(){

    LoaderMsg msg(LoaderMsg::MAST_WORKER_ADD_REQ, getNextMsgId(), getHostName(), getPort());
    BufferUdp msgData;
    msg.serializeToData(msgData);
    // create the proto buffer
    lsst::qserv::proto::LdrMastWorkerAddReq protoBuf;
    protoBuf.set_workerip(getHostName());
    protoBuf.set_workerport(getPort());

    StringElement addWorkerBuf;
    protoBuf.SerializeToString(&(addWorkerBuf.element));
    addWorkerBuf.appendToData(msgData);

    sendBufferTo(getMasterHostName(), getMasterPort(), msgData);
}


void CentralWorker::testSendBadMessage() {
    uint16_t kind = 60200;
    LoaderMsg msg(kind, getNextMsgId(), getHostName(), getPort());
    LOGS(_log, LOG_LVL_INFO, "testSendBadMessage msg=" << msg);
    BufferUdp msgData(128);
    msg.serializeToData(msgData);
    sendBufferTo(getMasterHostName(), getMasterPort(), msgData);
}




void CentralMaster::addWorker(std::string const& ip, short port) {
    auto item = _workerList->addWorker(ip, port);
    if (item != nullptr) {
        _doList.addItem(item);
    }
}


}}} // namespace lsst::qserv::loader
