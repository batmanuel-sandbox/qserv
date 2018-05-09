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
#include "Central.h"

// system headers
#include <boost/asio.hpp>
#include <iostream>

// Third-party headers


// qserv headers
#include "loader/LoaderMsg.h"
#include "proto/ProtoImporter.h"
#include "proto/loader.pb.h"


// LSST headers
#include "lsst/log/Log.h"


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


void CentralMaster::addWorker(std::string const& ip, int port) {
    // LOGS(_log, LOG_LVL_INFO, "&&& Master::addWorker");
    auto item = _mWorkerList->addWorker(ip, port);

    if (item != nullptr) {
        // If that was the first worker added, it gets unlimited range.
        if (_firstWorkerRegistered.exchange(true) == false) {
            LOGS(_log, LOG_LVL_INFO, "setAllInclusiveRange for name=" << item->getName());
            item->setAllInclusiveRange();
        }

        // TODO &&& maybe flag worker as active somehow ???

        item->addDoListItems(this);
        LOGS(_log, LOG_LVL_INFO, "Master::addWorker " << *item);
    }
}


void CentralMaster::updateNeighbors(uint32_t workerName, NeighborsInfo nInfo) {
    auto item = getWorkerNamed(workerName);
    int status = item->setKeyCounts(nInfo);
    if (status < 0) {
        LOGS(_log, LOG_LVL_WARN, "CentralMaster::updateNeighbors, unexpected neighbors");
        // TODO check if reasonable, otherwise figure out what went wrong and fix it.
        // Wait for things to converge.
        return;
    }
    _assignNeighborIfNeeded();
}


MWorkerListItem::Ptr CentralMaster::getWorkerNamed(uint32_t name) {
    return _mWorkerList->getWorkerNamed(name);
}


void CentralMaster::reqWorkerKeysInfo(uint64_t msgId, std::string const& ip, short port,
                                     std::string const& ourHostName, short ourPort) {
    LoaderMsg reqMsg(LoaderMsg::WORKER_KEYS_INFO_REQ, msgId, ourHostName, ourPort);
    BufferUdp data;
    reqMsg.serializeToData(data);
    sendBufferTo(ip, port, data);
}



std::ostream& operator<<(std::ostream& os, ChunkSubchunk csc) {
    os << "chunk=" << csc.chunk << " subchunk=" << csc.subchunk;
    return os;
}


}}} // namespace lsst::qserv::loader
