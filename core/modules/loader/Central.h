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
#ifndef LSST_QSERV_LOADER_CENTRAL_H_
#define LSST_QSERV_LOADER_CENTRAL_H_

// system headers
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <thread>
#include <vector>

// Qserv headers
#include "loader/ClientServer.h"
#include "loader/MasterServer.h"
#include "loader/MWorkerList.h"
#include "loader/WWorkerList.h"
#include "loader/WorkerServer.h"
#include "proto/loader.pb.h"
#include "util/ThreadPool.h"



namespace lsst {
namespace qserv {
namespace loader {

/// &&& TODO add fileId and row to this so it can be checked in _workerKeyInsertReq ?
struct ChunkSubchunk {
    ChunkSubchunk(int chunk_, int subchunk_) : chunk(chunk_), subchunk(subchunk_) {}
    int const chunk;
    int const subchunk;
    friend std::ostream& operator<<(std::ostream& os, ChunkSubchunk csc);
};


/// &&& Initially, just setting this up as Central for the worker, but may work for both Worker and Master.
/// &&& This class is central to loader workers and the master.
/// &&& It is the base class for WorkerCentral and MasterCentral
// This class is 'central' to the execution of the program, and must be around
// until the bitter end. As such, it can be accessed by normal pointers.
class Central {
public:
    Central(boost::asio::io_service& ioService,
            std::string const& masterHostName, int masterPort)
        : _ioService(ioService), _masterHostName(masterHostName), _masterPort(masterPort),
          _checkDoListThread([this](){ _checkDoList(); }){}

    Central() = delete;

    virtual ~Central();

    void run();

    std::string getMasterHostName() const { return _masterHostName; }
    int getMasterPort() const { return _masterPort; }

    uint64_t getNextMsgId() { return _sequence++; }

    int getErrCount() const { return _server->getErrCount(); }

    // WorkerList::Ptr getWorkerList() const { return _workerList; } &&&

    void sendBufferTo(std::string const& host, int port, BufferUdp& sendBuf) {
        _server->sendBufferTo(host, port, sendBuf);
    }

    // Only allow tracked commands on the queue
    void queueCmd(util::CommandTracked::Ptr const& cmd) {
        _queue->queCmd(cmd);
    }

    bool addDoListItem(DoListItem::Ptr const& item) {
        return _doList.addItem(item);
    }

    bool runAndAddDoListItem(DoListItem::Ptr const& item) {
        _doList.runItemNow(item);
        return _doList.addItem(item);
    }


    virtual std::string getOurLogId() { return "baseclass"; }

protected:
    /// Repeatedly check the items on the _doList.
    void _checkDoList();

    boost::asio::io_service& _ioService;

    /// Initialization order is important.
    DoList _doList{*this}; ///< List of items to be checked at regular intervals.

    std::string _masterHostName;    // &&& struct to keep hostName + port (WorkerList.h->NetworkAddress?)
    int _masterPort;
    // WorkerList::Ptr _workerList{new WorkerList(this)}; // &&& May not need to be a pointer. &&&
    
    std::atomic<uint64_t> _sequence{1};

    util::CommandQueue::Ptr _queue{new util::CommandQueue()};
    util::ThreadPool::Ptr _pool{util::ThreadPool::newThreadPool(10, _queue)};

    std::vector<std::thread> _ioServiceThreads; ///< List of asio io threads created by this

    ServerUdpBase::Ptr _server;

    bool _loop{true};
    std::thread _checkDoListThread;
};


class CentralWorker : public Central {
public:
    CentralWorker(boost::asio::io_service& ioService,
                  std::string const& masterHostName, int masterPort,
                  std::string const& hostName,       int port)
        : Central(ioService, masterHostName, masterPort),
          _hostName(hostName), _port(port) {
        _server = std::make_shared<WorkerServer>(_ioService, _hostName, _port, this);
        _monitorWorkers();
    }

    ~CentralWorker() override { _wWorkerList.reset(); }

    WWorkerList::Ptr getWorkerList() const { return _wWorkerList; }

    std::string getHostName() const { return _hostName; }
    int getPort() const { return _port; }

    void registerWithMaster();

    bool workerInfoReceive(BufferUdp::Ptr const&  data); // &&& spelling
    bool workerKeyInsertReq(LoaderMsg const& inMsg, BufferUdp::Ptr const&  data);

    bool isOurNameInvalid() const {
        std::lock_guard<std::mutex> lck(_ourNameMtx);
        return _ourNameInvalid;
    }

    bool setOurName(uint32_t name) {
        std::lock_guard<std::mutex> lck(_ourNameMtx);
        if (_ourNameInvalid) {
            _ourName = name;
            _ourNameInvalid = false;
            return true;
        } else {
            /// &&& add error message, check if _ourname matches name
            return false;
        }
    }

    uint32_t getOurName() const {
        std::lock_guard<std::mutex> lck(_ourNameMtx);
        return _ourName;
    }

    /// &&& TODO this is only needed for initial testing and should be deleted.
    std::string getOurLogId() override;

    void testSendBadMessage();

private:
    void _registerWithMaster();
    void _monitorWorkers();

    void _workerInfoReceive(std::unique_ptr<proto::WorkerListItem>& protoBuf);
    void _workerKeyInsertReq(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfoInsert>& protoBuf);
    void _forwardKeyInsertRequest(WWorkerListItem::Ptr const& target, LoaderMsg const& inMsg,
                                  std::unique_ptr<proto::KeyInfoInsert> const& protoData);


    const std::string _hostName;
    const int         _port;
    WWorkerList::Ptr _wWorkerList{new WWorkerList(this)};

    bool _ourNameInvalid{true}; ///< true until the name has been set by the master.
    uint32_t _ourName; ///< name given to us by the master
    mutable std::mutex _ourNameMtx; ///< protects _ourNameInvalid, _ourName

    // TODO _range both int and string;
    StringRange _strRange;
    // TODO _directorIdMap
    std::mutex _idMapMtx; ///< protect _rangeStr and _directorIdMap

    std::map<std::string, ChunkSubchunk> _directorIdMap;

};



class CentralMaster : public Central {
public:
    CentralMaster(boost::asio::io_service& ioService,
                  std::string const& masterHostName, int masterPort)
        : Central(ioService, masterHostName, masterPort) {
        _server = std::make_shared<MasterServer>(_ioService, masterHostName, masterPort, this);
    }

    ~CentralMaster() override { _mWorkerList.reset(); }

    void addWorker(std::string const& ip, int port);
    MWorkerListItem::Ptr getWorkerNamed(uint32_t name);
    void setRangeUnlimited();

    MWorkerList::Ptr getWorkerList() const { return _mWorkerList; }

    std::string getOurLogId() override { return "master"; }

private:
    MWorkerList::Ptr _mWorkerList{new MWorkerList(this)};

    std::atomic<bool> _firstWorkerRegistered{false};
};


/// TODO Maybe base this one CentralWorker or have a common base class?
class CentralClient : public Central {
public:
    CentralClient(boost::asio::io_service& ioService,
                  std::string const& masterHostName, int masterPort,
                  std::string const& workerHostName, int workerPort,
                  std::string const& hostName, int port)
        : Central(ioService, masterHostName, masterPort),
          _workerHostName(workerHostName), _workerPort(workerPort),
          _hostName(hostName), _port(port) {
        _server = std::make_shared<ClientServer>(_ioService, _hostName, _port, this);
    }

    ~CentralClient() override = default;

    std::string getHostName() const { return _hostName; }
    int getPort() const { return _port; }

    std::string getWorkerHostName() const { return _workerHostName; }
    int getWorkerPort() const { return _workerPort; }


    void keyInsertReq(std::string const& key, int chunk, int subchunk);

    void handleKeyInfo(LoaderMsg const& inMsg, BufferUdp::Ptr const& data);
    void handleKeyInsertComplete(LoaderMsg const& inMsg, BufferUdp::Ptr const& data);

    std::string getOurLogId() override { return "client"; }

private:
    void _keyInsertReq(std::string const& key, int chunk, int subchunk);
    void _handleKeyInsertComplete(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfo>& protoBuf);

    const std::string _workerHostName;
    const int         _workerPort;
    const std::string _hostName;
    const int         _port;


    /// It should keep trying this until it works, and then drop it from _waitingKeyMap.
    struct KeyInsertReqOneShot : public DoListItem {
        using Ptr = std::shared_ptr<KeyInsertReqOneShot>;
        struct CommandData {
            CommandData(CentralClient* central_, std::string const& key_, int chunk_, int subchunk_) :
                central(central_), key(key_), chunk(chunk_), subchunk(subchunk_) {}
            CentralClient* central;
            std::string const& key;
            int chunk;
            int subchunk;
        };

        KeyInsertReqOneShot(CentralClient* central_, std::string const& key_, int chunk_, int subchunk_) :
                   cmdData(central_, key_, chunk_, subchunk_) {
                   _oneShot = true;
               }

        CommandData cmdData;
        util::CommandTracked::Ptr createCommand() override {
            struct KeyInsertReqCmd : public util::CommandTracked {
                KeyInsertReqCmd(CommandData& cd) : cData(cd) {}
                void action(util::CmdData*) override {
                    cData.central->_keyInsertReq(cData.key, cData.chunk, cData.subchunk);
                }
                CommandData cData;
            };
            return std::make_shared<KeyInsertReqCmd>(cmdData);
        }
    };


    std::map<std::string, KeyInsertReqOneShot::Ptr> _waitingKeyMap;
    std::mutex _waitingKeyMtx; ///< protects _waitingKeyMap
};


}}} // namespace lsst::qserv::loader


#endif // LSST_QSERV_LOADER_CENTRAL_H_
