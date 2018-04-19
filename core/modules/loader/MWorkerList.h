// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
 *
 */
#ifndef LSST_QSERV_LOADER_MWORKERLIST_H_
#define LSST_QSERV_LOADER_MWORKERLIST_H_

// system headers
#include <atomic>
#include <map>
#include <memory>
#include <mutex>

// Qserv headers
#include "loader/BufferUdp.h"
#include "loader/DoList.h"
#include "loader/NetworkAddress.h"
#include "loader/StringRange.h"


namespace lsst {
namespace qserv {
namespace loader {

class Central;
class CentralWorker;
class CentralMaster;
class LoaderMsg;



/// Standard information for a single worker, IP address, key range, timeouts.
class MWorkerListItem : public std::enable_shared_from_this<MWorkerListItem> {
public:
    using Ptr = std::shared_ptr<MWorkerListItem>;
    using WPtr = std::weak_ptr<MWorkerListItem>;

    static MWorkerListItem::Ptr create(uint32_t name, CentralMaster *central) {
        return MWorkerListItem::Ptr(new MWorkerListItem(name, central));
    }
    static MWorkerListItem::Ptr create(uint32_t name, NetworkAddress const& address, CentralMaster *central) {
        return MWorkerListItem::Ptr(new MWorkerListItem(name, address, central));
    }


    MWorkerListItem() = delete;
    MWorkerListItem(MWorkerListItem const&) = delete;
    MWorkerListItem& operator=(MWorkerListItem const&) = delete;

    virtual ~MWorkerListItem() = default;

    NetworkAddress getAddress() const { return _address; }
    uint32_t getName() const { return _name; }
    StringRange getRangeString() const { return _range; }

    void addDoListItems(Central *central);

    void flagNeedToSendList();

    util::CommandTracked::Ptr createCommandMaster(CentralMaster* centralM);

    void sendListToWorkerInfoReceived();

    friend std::ostream& operator<<(std::ostream& os, MWorkerListItem const& item);
private:
    MWorkerListItem(uint32_t name, CentralMaster* central) : _name(name), _central(central) {}
    MWorkerListItem(uint32_t name, NetworkAddress const& address, CentralMaster* central)
         : _name(name), _address(address), _central(central) {}

    uint32_t _name;
    NetworkAddress _address{"", 0}; ///< empty string indicates address is not valid.
    TimeOut _lastContact{std::chrono::minutes(10)};  ///< Last time information was received from this worker
    StringRange _range;  ///< min and max range for this worker.

    CentralMaster* _central;

    // Occasionally send a list of all workers to the worker represented by this object.
    struct SendListToWorker : public DoListItem {
        SendListToWorker(MWorkerListItem::Ptr const& mWorkerListItem_, CentralMaster *central_) :
            mWorkerListItem(mWorkerListItem_), central(central_) {}
        MWorkerListItem::WPtr mWorkerListItem;
        CentralMaster *central;
        util::CommandTracked::Ptr createCommand() override;
    };
    DoListItem::Ptr _sendListToWorker;
    std::mutex _doListItemsMtx; ///< protects _sendListToWorker
};



class MWorkerList : public DoListItem {
public:
    using Ptr = std::shared_ptr<MWorkerList>;

    MWorkerList(CentralMaster* central) : _central(central) {}
    MWorkerList() = delete;
    MWorkerList(MWorkerList const&) = delete;
    MWorkerList& operator=(MWorkerList const&) = delete;

    virtual ~MWorkerList() = default;

    ///// Master only //////////////////////
    // Returns pointer to new item if an item was created.
    MWorkerListItem::Ptr addWorker(std::string const& ip, short port);

    // Returns true of message could be parsed and a send will be attempted.
    bool sendListTo(uint64_t msgId, std::string const& ip, short port,
                    std::string const& outHostName, short ourPort);

    util::CommandTracked::Ptr createCommand() override;
    // util::CommandTracked::Ptr createCommandWorker(CentralWorker* centralW); &&&
    util::CommandTracked::Ptr createCommandMaster(CentralMaster* centralM);

    //////////////////////////////////////////
    /// Nearly the same on Worker and Master
    size_t getNameMapSize() {
        std::lock_guard<std::mutex> lck(_mapMtx);
        return _nameMap.size();
    }

    MWorkerListItem::Ptr getWorkerNamed(uint32_t name) {
        std::lock_guard<std::mutex> lck(_mapMtx);
        auto iter = _nameMap.find(name);
        if (iter == _nameMap.end()) { return nullptr; }
        return iter->second;
    }

protected:
    void _flagListChange();

    CentralMaster* _central;
    std::map<uint32_t, MWorkerListItem::Ptr> _nameMap;
    std::map<NetworkAddress, MWorkerListItem::Ptr> _ipMap;
    bool _wListChanged{false}; ///< true if the list has changed
    BufferUdp::Ptr _stateListData; ///< message
    uint32_t _totalNumberOfWorkers{0}; ///< total number of workers according to the master.
    std::mutex _mapMtx; ///< protects _nameMap, _ipMap, _wListChanged

    std::atomic<uint32_t> _sequence{1};

};


}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_MWORKERLIST_H_
