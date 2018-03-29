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
#ifndef LSST_QSERV_LOADER_WORKERLIST_H_
#define LSST_QSERV_LOADER_WORKERLIST_H_

// system headers
#include <atomic>
#include <map>
#include <memory>
#include <mutex>

// Qserv headers
#include "loader/BufferUdp.h"
#include "loader/WorkerList.h"


namespace lsst {
namespace qserv {
namespace loader {

class LoaderMsg;

/// Comparable network addresses.
struct NetworkAddress {
    NetworkAddress(std::string const& ip_, short port_) : ip(ip_), port(port_) {}
    NetworkAddress() = delete;
    NetworkAddress(NetworkAddress const&) = default;

    const std::string ip;
    const short port; // Most of the workers will have the same port number.

    bool operator==(NetworkAddress const& other) const {
        return(port == other.port && ip == other.ip);
    }

    bool operator<(NetworkAddress const& other) const {
        auto compRes = ip.compare(other.ip);
        if (compRes < 0) { return true; }
        if (compRes > 0) { return false; }
        return port < other.port;
    }

    bool operator>(NetworkAddress const& other) const {
        return (other < *this);
    }

    friend std::ostream& operator<<(std::ostream& os, NetworkAddress const& adr);
};


class WorkerListItem {
public:
    using Ptr = std::shared_ptr<WorkerListItem>;

    WorkerListItem(uint32_t name) : _name(name) {}
    WorkerListItem(uint32_t name, NetworkAddress const& address)
        : _name(name), _address(address) {}

    WorkerListItem() = delete;
    WorkerListItem(WorkerListItem const&) = delete;
    WorkerListItem& operator=(WorkerListItem const&) = delete;

    virtual ~WorkerListItem() = default;

    NetworkAddress getAddress() const { return _address; }
    uint32_t getName() const { return _name; }

    friend std::ostream& operator<<(std::ostream& os, WorkerListItem const& item);
private:
    uint32_t _name;
    NetworkAddress _address{"", 0}; ///< empty string indicates address is not valid.
    // _lastContact; &&&
    // _lastUpdate; &&&
    // _range; &&&
};

class WorkerList {
public:
    using Ptr = std::shared_ptr<WorkerList>;

    WorkerList() = default;
    WorkerList(WorkerList const&) = delete;
    WorkerList& operator=(WorkerList const&) = delete;

    virtual ~WorkerList() = default;

    // Returns true when new worker added
    bool addWorker(std::string const& ip, short port);

    // Returns true of message could be parsed and send will be attempted.
    bool sendListTo(uint64_t msgId, std::string const& ip, short port,
                    std::string const& outHostName, short ourPort);

    bool workerListReceive(BufferUdp::Ptr const& data);

protected:
    void _flagListChange();

    std::map<uint32_t, WorkerListItem::Ptr> _nameMap;
    std::map<NetworkAddress, WorkerListItem::Ptr> _ipMap;
    bool _wListChanged{false}; ///< true if the list has changed
    BufferUdp::Ptr _stateListData; ///< message
    uint32_t _totalNumberOfWorkers{0}; ///< total number of workers according to the master.
    std::mutex _mapMtx; ///< protects _nameMap, _ipMap, _wListChanged

    std::atomic<uint32_t> _sequence{1};
};


}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_WORKERLIST_H_
