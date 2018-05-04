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


bool CentralWorker::workerInfoReceive(BufferUdp::Ptr const&  data) {
    // LOGS(_log, LOG_LVL_INFO, " ******&&& workerInfoRecieve data=" << data->dump());
    // Open the data protobuffer and add it to our list.
    StringElement::Ptr sData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (sData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralWorker::workerInfoRecieve Failed to parse list");
        return false;
    }
    std::unique_ptr<proto::WorkerListItem> protoList = sData->protoParse<proto::WorkerListItem>();
    if (protoList == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralWorker::workerInfoRecieve Failed to parse list");
        return false;
    }

    // &&& TODO move this call to another thread
    _workerInfoReceive(protoList);
    return true;
}


void CentralWorker::_workerInfoReceive(std::unique_ptr<proto::WorkerListItem>& protoL) {
    std::unique_ptr<proto::WorkerListItem> protoList(std::move(protoL));


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
}


bool CentralWorker::workerKeyInsertReq(LoaderMsg const& inMsg, BufferUdp::Ptr const&  data) {
    StringElement::Ptr sData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (sData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralWorker::workerKeyInsertReq Failed to parse list");
        return false;
    }
    auto protoData = sData->protoParse<proto::KeyInfoInsert>();
    if (protoData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralWorker::workerKeyInsertReq Failed to parse list");
        return false;
    }

    // &&& TODO move this to another thread
    _workerKeyInsertReq(inMsg, protoData);
    return true;
}


void CentralWorker::_workerKeyInsertReq(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfoInsert>& protoBuf) {
    std::unique_ptr<proto::KeyInfoInsert> protoData(std::move(protoBuf));

    // Get the source of the request
    proto::LdrNetAddress protoAddr = protoData->requester();
    NetworkAddress nAddr(protoAddr.workerip(), protoAddr.workerport());

    proto::KeyInfo protoKeyInfo = protoData->keyinfo();
    std::string key = protoKeyInfo.key();
    ChunkSubchunk chunkInfo(protoKeyInfo.chunk(), protoKeyInfo.subchunk());

    /// &&& see if the key should be inserted into our map
    std::unique_lock<std::mutex> lck(_idMapMtx);
    if (_strRange.isInRange(key)) {
        // insert into our map
        auto res = _directorIdMap.insert(std::make_pair(key, chunkInfo));
        lck.unlock();
        if (not res.second) {
            // &&& element already found, check file id and row number. Bad if not the same.
            // TODO send back duplicate key mismatch message to the original requester and return
        }
        LOGS(_log, LOG_LVL_INFO, "Key inserted=" << key << "(" << chunkInfo << ")");
        // TODO Send this item to the keyLogger (which would then send KEY_INSERT_COMPLETE back to the requester),
        // for now this function will send the message back for proof of concept.
        LoaderMsg msg(LoaderMsg::KEY_INSERT_COMPLETE, inMsg.msgId->element, getHostName(), getPort());
        BufferUdp msgData;
        msg.serializeToData(msgData);
        // protoKeyInfo should still be the same
        proto::KeyInfo protoReply;
        protoReply.set_key(key);
        protoReply.set_chunk(chunkInfo.chunk);
        protoReply.set_subchunk(chunkInfo.subchunk);
        StringElement strElem;
        protoReply.SerializeToString(&(strElem.element));
        strElem.appendToData(msgData);
        LOGS(_log, LOG_LVL_INFO, "&&& sending complete " << key << " to " << nAddr << " from " << _ourName);
        sendBufferTo(nAddr.ip, nAddr.port, msgData);
    } else {
        // &&& TODO find the target range in the list and send the request there
        auto targetWorker = _wWorkerList->findWorkerForKey(key);
        if (targetWorker == nullptr) { return; }
        _forwardKeyInsertRequest(targetWorker, inMsg, protoData);
    }
}


void CentralWorker::_forwardKeyInsertRequest(WWorkerListItem::Ptr const& target, LoaderMsg const& inMsg,
                                             std::unique_ptr<proto::KeyInfoInsert> const& protoData) {
    // The proto buffer should be the same, just need a new message.
    LoaderMsg msg(LoaderMsg::KEY_INSERT_REQ, inMsg.msgId->element, getHostName(), getPort());
    BufferUdp msgData;
    msg.serializeToData(msgData);

    StringElement strElem;
    protoData->SerializeToString(&(strElem.element));
    strElem.appendToData(msgData);

    auto nAddr = target->getAddress();
    sendBufferTo(nAddr.ip, nAddr.port, msgData);
}


bool CentralWorker::workerKeyInfoReq(LoaderMsg const& inMsg, BufferUdp::Ptr const&  data) {
    StringElement::Ptr sData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (sData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralWorker::workerKeyInfoReq Failed to parse list");
        return false;
    }
    auto protoData = sData->protoParse<proto::KeyInfoInsert>();  /// &&& KeyInfoInsert <- more generic name or new type for key lookup?
    if (protoData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "CentralWorker::workerKeyInfoReq Failed to parse list");
        return false;
    }

    // &&& TODO move this to another thread
    _workerKeyInfoReq(inMsg, protoData);
    return true;
}


// &&& alter
void CentralWorker::_workerKeyInfoReq(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfoInsert>& protoBuf) {
    std::unique_ptr<proto::KeyInfoInsert> protoData(std::move(protoBuf));

    // Get the source of the request
    proto::LdrNetAddress protoAddr = protoData->requester();
    NetworkAddress nAddr(protoAddr.workerip(), protoAddr.workerport());

    proto::KeyInfo protoKeyInfo = protoData->keyinfo();
    std::string key = protoKeyInfo.key();
    //    ChunkSubchunk chunkInfo(protoKeyInfo.chunk(), protoKeyInfo.subchunk());  &&&

    /// &&& see if the key is in our map
    std::unique_lock<std::mutex> lck(_idMapMtx);
    if (_strRange.isInRange(key)) {
        // check out map
        auto iter = _directorIdMap.find(key);
        lck.unlock();

        // Key found or not, message will be returned.
        LoaderMsg msg(LoaderMsg::KEY_INFO, inMsg.msgId->element, getHostName(), getPort());
        BufferUdp msgData;
        msg.serializeToData(msgData);
        proto::KeyInfo protoReply;
        protoReply.set_key(key);
        if (iter == _directorIdMap.end()) {
            // key not found message.
            protoReply.set_chunk(0);
            protoReply.set_subchunk(0);
            protoReply.set_success(false);
            LOGS(_log, LOG_LVL_INFO, "Key info not found key=" << key);
        } else {
            // key found message.
            auto elem = iter->second;
            protoReply.set_chunk(elem.chunk);
            protoReply.set_subchunk(elem.subchunk);
            protoReply.set_success(true);
            LOGS(_log, LOG_LVL_INFO, "Key info lookup key=" << key <<
                 " (" << protoReply.chunk() << ", " << protoReply.subchunk() << ")");
        }
        StringElement strElem;
        protoReply.SerializeToString(&(strElem.element));
        strElem.appendToData(msgData);
        LOGS(_log, LOG_LVL_INFO, "&&& sending key lookup " << key << " to " << nAddr << " from " << _ourName);
        sendBufferTo(nAddr.ip, nAddr.port, msgData);
    } else {
        // Find the target range in the list and send the request there
        auto targetWorker = _wWorkerList->findWorkerForKey(key);
        if (targetWorker == nullptr) { return; } // Client will have to try again.
        _forwardKeyInfoRequest(targetWorker, inMsg, protoData);
    }
}


// &&& alter
// TODO This looks a lot like the other _forward*** functions, try to combine them.
void CentralWorker::_forwardKeyInfoRequest(WWorkerListItem::Ptr const& target, LoaderMsg const& inMsg,
                                             std::unique_ptr<proto::KeyInfoInsert> const& protoData) {
    // The proto buffer should be the same, just need a new message.
    LoaderMsg msg(LoaderMsg::KEY_INFO_REQ, inMsg.msgId->element, getHostName(), getPort());
    BufferUdp msgData;
    msg.serializeToData(msgData);

    StringElement strElem;
    protoData->SerializeToString(&(strElem.element));
    strElem.appendToData(msgData);

    auto nAddr = target->getAddress();
    sendBufferTo(nAddr.ip, nAddr.port, msgData);
}




void CentralWorker::_registerWithMaster() {

    LoaderMsg msg(LoaderMsg::MAST_WORKER_ADD_REQ, getNextMsgId(), getHostName(), getPort());
    BufferUdp msgData;
    msg.serializeToData(msgData);
    // create the proto buffer
    lsst::qserv::proto::LdrNetAddress protoBuf;
    protoBuf.set_workerip(getHostName());
    protoBuf.set_workerport(getPort());

    StringElement strElem;
    protoBuf.SerializeToString(&(strElem.element));
    strElem.appendToData(msgData);

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




std::ostream& operator<<(std::ostream& os, ChunkSubchunk csc) {
    os << "chunk=" << csc.chunk << " subchunk=" << csc.subchunk;
    return os;
}


}}} // namespace lsst::qserv::loader
