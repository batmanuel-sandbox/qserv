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


std::string CentralWorker::getOurLogId() {
    std::stringstream os;
    os << "(w name=" << _ourName << " addr=" << _hostName << ":" << _port << ")";
    return os.str();
}

void CentralWorker::_monitorWorkers() {
    // Add _workerList to _doList so it starts checking new entries.
    // LOGS(_log, LOG_LVL_INFO, "&&& CentralWorker::_monitorWorkers()");
    _doList.addItem(_wWorkerList);
}


void CentralWorker::registerWithMaster() {
    // &&& TODO: add a one shot DoList item to keep calling _registerWithMaster until we have our name.
    _registerWithMaster();
}


bool CentralWorker::workerInfoRecieve(BufferUdp::Ptr const&  data) {
    // LOGS(_log, LOG_LVL_INFO, " ******&&& workerInfoRecieve data=" << data->dump());
    // Open the data protobuffer and add it to our list.
    StringElement::Ptr sData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (sData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralWorker::workerInfoRecieve Failed to parse list");
        return false;
    }
    auto protoList = sData->protoParse<proto::WorkerListItem>();
    if (protoList == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralWorker::workerInfoRecieve Failed to parse list");
        return false;
    }

    // &&& TODO move this to another thread
    // Check the information, if it is our network address, set or check our name.
    // Then compare it with the map, adding new/changed information.
    uint32_t name = protoList->name();
    std::string ip("");
    int port = 0;
    if (protoList->has_address()) {
        proto::LdrNetAddress protoAddr = protoList->address();
        ip = protoAddr.workerip();
        port = protoAddr.workerport();
    }
    StringRange strRange;
    if (protoList->has_rangestr()) {
        proto::WorkerRangeString protoRange= protoList->rangestr();
        bool valid        = protoRange.valid();
        if (valid) {
            std::string min   = protoRange.min();
            std::string max   = protoRange.max();
            bool unlimited = protoRange.maxunlimited();
            strRange.setMinMax(min, max, unlimited);
            //LOGS(_log, LOG_LVL_WARN, "&&& CentralWorker::workerInfoRecieve range=" << strRange);
        }
    }

    // If the address matches ours, check the name.
    if (getHostName() == ip && getPort() == port) {
        if (isOurNameInvalid()) {
            LOGS(_log, LOG_LVL_INFO, "Setting our name " << name);
            setOurName(name);
        } else if (getOurName() != name) {
            LOGS(_log, LOG_LVL_ERROR, "Our name doesn't match address from master! name=" <<
                                      getOurName() << " masterName=" << name);
        }

        // It is this worker. If there is a valid range in the message and our range is not valid,
        // take the range given as our own. This should only ever happen with the all inclusive range.
        // when this is the first worker being registered.
        if (strRange.getValid()) {
            std::lock_guard<std::mutex> lckM(_idMapMtx);
            if (not _strRange.getValid()) {
                LOGS(_log, LOG_LVL_INFO, "Setting our range " << strRange);
                _strRange.setMinMax(strRange.getMin(), strRange.getMax(), strRange.getUnlimited());
            }
        }
    }

    // Make/update entry in map.
    _wWorkerList->updateEntry(name, ip, port, strRange);

    return true;
}


void CentralWorker::_registerWithMaster() {

    LoaderMsg msg(LoaderMsg::MAST_WORKER_ADD_REQ, getNextMsgId(), getHostName(), getPort());
    BufferUdp msgData;
    msg.serializeToData(msgData);
    // create the proto buffer
    lsst::qserv::proto::LdrNetAddress protoBuf;
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


MWorkerListItem::Ptr CentralMaster::getWorkerNamed(uint32_t name) {
    return _mWorkerList->getWorkerNamed(name);
}


}}} // namespace lsst::qserv::loader
