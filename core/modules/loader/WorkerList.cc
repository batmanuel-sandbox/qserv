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
#include "loader/WorkerList.h"

// System headers
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
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.WorkerList");
}

namespace lsst {
namespace qserv {
namespace loader {

// Returns true when new worker added
bool WorkerList::addWorker(std::string const& ip, short port) {
    NetworkAddress address(ip, port);
    {
        // If it is already in the map, do not change its name.
        std::lock_guard<std::mutex> lock(_mapMtx);
        auto iter = _ipMap.find(address);
        if (iter != _ipMap.end()) {
            LOGS(_log, LOG_LVL_WARN, "addWorker, Could not add worker as worker already exists. " <<
                    ip << ":" << port);
            return false;
        }
        // Get an id and make new worker item
        auto workerListItem = std::make_shared<WorkerListItem>(_sequence++, address);
        _ipMap.insert(std::make_pair(address, workerListItem));
        _nameMap.insert(std::make_pair(workerListItem->getName(), workerListItem));
        LOGS(_log, LOG_LVL_INFO, "Added worker " << *workerListItem);
        _flagListChange();
    }

    // TODO: adding a worker changes state and messages to workers should be sent
    return true;
}


bool WorkerList::sendListTo(uint64_t msgId, std::string const& ip, short port,
                            std::string const& ourHostName, short ourPort) {
    NetworkAddress address(ip, port);
    StringElement workerList;
    {
        std::lock_guard<std::mutex> lock(_mapMtx);
        if (_wListChanged || _stateListData == nullptr) {
            _wListChanged = false;
            /// At this time, all workers should easily fit in a single message.
            /// TODO send multiple messages (if needed) with each having the address and range of 100 workers.
            ///      This version is useful for testing. _stateListData becomes a vector.
            proto::LdrMastWorkerList protoList;
            protoList.set_workercount(_nameMap.size());
            for (auto const& item : _nameMap ) {
                proto::WorkerListItem* protoItem = protoList.add_worker();
                WorkerListItem::Ptr wListItem = item.second;
                protoItem->set_name(wListItem->getName());
            }
            protoList.SerializeToString(&(workerList.element));
            LoaderMsg workerListMsg(LoaderMsg::MAST_WORKER_LIST, msgId, ourHostName, ourPort);
            _stateListData = std::make_shared<BufferUdp>();
            workerListMsg.serializeToData(*_stateListData);
            workerList.appendToData(*_stateListData);
        }
    }


    // TODO: &&&(creating a client socket here is odd. Should use master socket to send or make a pool of contexts (pool of agents with contexts?)
    {
        using namespace boost::asio;
        io_context ioContext;
        ip::udp::resolver resolver(ioContext);
        ip::udp::socket socket(ioContext);
        socket.open(ip::udp::v4());
        ip::udp::endpoint endpoint = *resolver.resolve(ip::udp::v4(), ip, std::to_string(port)).begin(); // there has got to be a better way &&&
        socket.send_to(buffer(_stateListData->begin(), _stateListData->getCurrentWriteLength()), endpoint);
    }
    return true;
}


bool WorkerList::workerListReceive(BufferUdp::Ptr const& data) {
    LOGS(_log, LOG_LVL_INFO, "***************&&& workerListReceive data=" << data->dump());
    // &&& break open the data protobuffer and add it to our list.
    proto::LdrMastWorkerList protoList;
    StringElement::Ptr sData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    bool success = proto::ProtoImporter<proto::LdrMastWorkerList>::setMsgFrom(protoList, sData->element.data(), sData->element.length());
    if (not success) {
        LOGS(_log, LOG_LVL_WARN, "WorkerList::workerListReceive Failed to parse list");
        return false;
    }

    int sizeChange = 0;
    std::string strNames;
    {
        std::lock_guard<std::mutex> lock(_mapMtx);
        size_t initialSize = _nameMap.size();
        _totalNumberOfWorkers = protoList.workercount(); // there may be more workers than will fit in a message.
        int sz = protoList.worker_size();

        for (int j=0; j < sz; ++j) {
            proto::WorkerListItem const& protoItem = protoList.worker(j);
            uint32_t name = protoItem.name();
            auto iter = _nameMap[name];
            if (iter == nullptr) {
                iter = std::make_shared<WorkerListItem>(name);
                strNames += std::to_string(name) + ",";
            }
        }
        sizeChange = _nameMap.size() - initialSize;
        if (sizeChange > 0) {
            _flagListChange();
        }
    }
    LOGS(_log, LOG_LVL_INFO, "workerListReceive added " << sizeChange << " names=" << strNames);

    return true;
}

// must lock _mapMtx before calling this function
void WorkerList::_flagListChange() {
    _wListChanged = true;
    // TODO: &&& indicate to every WorkerItem in the list that it needs to send a message to its worker.

}


std::ostream& operator<<(std::ostream& os, NetworkAddress const& adr) {
    os << "ip(" << adr.ip << ":" << adr.port << ")";
    return os;
}


std::ostream& operator<<(std::ostream& os, WorkerListItem const& item) {
    os << "name=" << item._name << " address=" << item._address;
    return os;
}


}}} // namespace lsst::qserv::loader
