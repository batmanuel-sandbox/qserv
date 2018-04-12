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
#include "loader/Central.h"
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

/* &&&
util::CommandTracked::Ptr WorkerListItem::createCommand() {

    CentralWorker* centralWorker = dynamic_cast<CentralWorker*>(_central);
    if (centralWorker != nullptr) {
        return createCommandWorker(centralWorker);
    }

    CentralMaster* centralMaster = dynamic_cast<CentralMaster*>(_central);
    if (centralMaster != nullptr) {
        return createCommandMaster(centralMaster);
    }
    return nullptr;
}
*/

util::CommandTracked::Ptr WorkerListItem::WorkerNeedsMasterData::createCommand() {
    auto item = workerListItem.lock();
    if (item == nullptr) {
        // TODO: should mark set the removal flag for this doListItem
        return nullptr;
    }
    CentralWorker* centralWorker = dynamic_cast<CentralWorker*>(item->_central);
        if (centralWorker != nullptr) {
            return item->createCommandWorker(centralWorker);
        }
}

util::CommandTracked::Ptr WorkerListItem::createCommandWorker(CentralWorker* centralW) {
     // Create a command to put on the pool to
     //  - create an io_contex
     //  - ask the master about a server with _name

    class WorkerReqCmd : public util::CommandTracked {
    public:
        WorkerReqCmd(CentralWorker* centralW, uint32_t name) : _centralW(centralW), _name(name) {}

        void action(util::CmdData *data) override {
            /// Request all information the master has for one worker.
            LOGS(_log, LOG_LVL_INFO, "&&& WorkerListItem::createCommand::WorkerReqCmd::action *******************");

            // TODO make a function for this, it's always going to be the same.
            proto::LdrNetAddress protoOurAddress;
            protoOurAddress.set_workerip(_centralW->getHostName());
            protoOurAddress.set_workerport(_centralW->getPort());
            StringElement eOurAddress(protoOurAddress.SerializeAsString());

            proto::WorkerListItem protoItem;
            protoItem.set_name(_name);
            StringElement eItem(protoItem.SerializeAsString());

            LoaderMsg workerInfoReqMsg(LoaderMsg::MAST_WORKER_INFO_REQ, _centralW->getNextMsgId(),
                                       _centralW->getHostName(), _centralW->getPort());
            BufferUdp sendBuf(1000);
            workerInfoReqMsg.serializeToData(sendBuf);
            eOurAddress.appendToData(sendBuf);
            eItem.appendToData(sendBuf);

            // Send the request to master.
            auto masterHost = _centralW->getMasterHostName();
            auto masterPort = _centralW->getMasterPort();
            _centralW->sendBufferTo(masterHost, masterPort, sendBuf);
        }

    private:
        CentralWorker* _centralW;
        uint32_t _name;
    };

    LOGS(_log, LOG_LVL_INFO, "&&& WorkerListItem::createCommandWorker ******************" << _name);
    return std::make_shared<WorkerReqCmd>(centralW, _name);
}


util::CommandTracked::Ptr WorkerListItem::createCommandMaster(CentralMaster* centralMaster) {
    LOGS(_log, LOG_LVL_ERROR, "&&& WorkerListItem::createCommandMaster This function needs to do something!!!!!!!!!");
    // &&& ask worker for current range, neighbors.
    return nullptr;
}


util::CommandTracked::Ptr WorkerList::createCommand() {
    CentralWorker* centralWorker = dynamic_cast<CentralWorker*>(_central);
      if (centralWorker != nullptr) {
          return createCommandWorker(centralWorker);
      }

      CentralMaster* centralMaster = dynamic_cast<CentralMaster*>(_central);
      if (centralMaster != nullptr) {
          return createCommandMaster(centralMaster);
      }
      return nullptr;
}


util::CommandTracked::Ptr WorkerList::createCommandWorker(CentralWorker* centralW) {
    // On the worker, need to occasionally ask for a list of workers from the master
    // and make sure each of those workers is on the doList
    class MastWorkerListReqCmd : public util::CommandTracked {
    public:
        MastWorkerListReqCmd(CentralWorker* centralW, std::map<uint32_t, WorkerListItem::Ptr> nameMap)
            : _centralW(centralW), _nameMap(nameMap) {}

        void action(util::CmdData *data) override {
            /// Request a list of all workers.
            LOGS(_log, LOG_LVL_INFO, "&&& WorkerListItem::createCommand::WorkerReqCmd::action");

            // TODO make a function for this, it's always going to be the same.
            proto::LdrNetAddress protoOurAddress;
            protoOurAddress.set_workerip(_centralW->getHostName());
            protoOurAddress.set_workerport(_centralW->getPort());
            StringElement eOurAddress(protoOurAddress.SerializeAsString());

            LoaderMsg workerInfoReqMsg(LoaderMsg::MAST_WORKER_LIST_REQ, _centralW->getNextMsgId(),
                                       _centralW->getHostName(), _centralW->getPort());
            BufferUdp sendBuf(1000);
            workerInfoReqMsg.serializeToData(sendBuf);
            eOurAddress.appendToData(sendBuf);

            // Send the request to master.
            auto masterHost = _centralW->getMasterHostName();
            auto masterPort = _centralW->getMasterPort();
            _centralW->sendBufferTo(masterHost, masterPort, sendBuf);

            /// Go through the existing list and add any that have not been add to the doList
            for (auto const& item : _nameMap) {
                item.second->addDoListItems(_centralW);
                //_centralW->addDoListItem(item.second);
            }
        }

    private:
        CentralWorker* _centralW;
        std::map<uint32_t, WorkerListItem::Ptr> _nameMap;
    };

    LOGS(_log, LOG_LVL_INFO, "&&& WorkerList::createCommandWorker");
    return std::make_shared<MastWorkerListReqCmd>(centralW, _nameMap);
}


util::CommandTracked::Ptr WorkerList::createCommandMaster(CentralMaster* centralM) {
    // &&& The master probably doesn't need to make any checks on the list, it just
    // &&& wants to make sure all of its items are on the doList.
    return nullptr;
}


// Returns true when new worker added
WorkerListItem::Ptr WorkerList::addWorker(std::string const& ip, short port) {
    NetworkAddress address(ip, port);

    // If it is already in the map, do not change its name.
    std::lock_guard<std::mutex> lock(_mapMtx);
    auto iter = _ipMap.find(address);
    if (iter != _ipMap.end()) {
        LOGS(_log, LOG_LVL_WARN, "addWorker, Could not add worker as worker already exists. " <<
                ip << ":" << port);
        return nullptr;
    }
    // Get an id and make new worker item
    auto workerListItem = WorkerListItem::create(_sequence++, address, _central);
    _ipMap.insert(std::make_pair(address, workerListItem));
    _nameMap.insert(std::make_pair(workerListItem->getName(), workerListItem));
    LOGS(_log, LOG_LVL_INFO, "Added worker " << *workerListItem);
    _flagListChange();

    return workerListItem;
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
    LOGS(_log, LOG_LVL_INFO, " ***&&& workerListReceive data=" << data->dump());
    // &&& break open the data protobuffer and add it to our list.
    /* &&&
    proto::LdrMastWorkerList protoList;
    StringElement::Ptr sData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));

    bool success = proto::ProtoImporter<proto::LdrMastWorkerList>::setMsgFrom(protoList, sData->element.data(), sData->element.length());
    if (not success) {
        LOGS(_log, LOG_LVL_WARN, "WorkerList::workerListReceive Failed to parse list");
        return false;
    }
    */
    StringElement::Ptr sData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (sData == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "WorkerList::workerListReceive Failed to parse list");
        return false;
    }
    auto protoList = sData->protoParse<proto::LdrMastWorkerList>();
    if (protoList == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "WorkerList::workerListReceive Failed to parse list");
        return false;
    }

    int sizeChange = 0;
    std::string strNames;
    {
        std::lock_guard<std::mutex> lock(_mapMtx);
        size_t initialSize = _nameMap.size();
        _totalNumberOfWorkers = protoList->workercount(); // There may be more workers than will fit in a message.
        int sz = protoList->worker_size();

        for (int j=0; j < sz; ++j) {
            proto::WorkerListItem const& protoItem = protoList->worker(j);
            uint32_t name = protoItem.name();
            // Most of the time, the worker will already be in the map.
            auto item = _nameMap[name];
            if (item == nullptr) {
                item = WorkerListItem::create(name, _central);
                _nameMap[name] = item;
                strNames += std::to_string(name) + ",";
                // _central->addDoListItem(item); &&&
                item->addDoListItems(_central);
            }
        }
        sizeChange = _nameMap.size() - initialSize;
        if (sizeChange > 0) {
            _flagListChange();
        }
    }

    infoReceived(); // Avoid asking for this info for a while.
    LOGS(_log, LOG_LVL_INFO, "workerListReceive added " << sizeChange << " names=" << strNames);

    return true;
}

// must lock _mapMtx before calling this function
void WorkerList::_flagListChange() {
    _wListChanged = true;
    // TODO: &&& on Master only, flag each worker in the list that it needs to send an updated list to it's worker.
}

void WorkerListItem::addDoListItems(Central *central) {
    if (_workerUpdateNeedsMasterData == nullptr) {
        _workerUpdateNeedsMasterData.reset(new WorkerNeedsMasterData(shared_from_this()));
        central->addDoListItem(_workerUpdateNeedsMasterData);
    }
}


std::ostream& operator<<(std::ostream& os, WorkerListItem const& item) {
    os << "name=" << item._name << " address=" << item._address;
    return os;
}


}}} // namespace lsst::qserv::loader
