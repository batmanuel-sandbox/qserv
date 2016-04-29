// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 LSST Corporation.
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
#include "xrdsvc/SsiService.h"

// System headers
#include <cassert>
#include <iostream>
#include <string>
#include <stdlib.h>
#include <unistd.h>

// Third-party headers
#include "XProtocol/XProtocol.hh"
#include "XrdSsi/XrdSsiLogger.hh"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "memman/MemMan.h"
#include "memman/MemManNone.h"
#include "sql/SqlConnection.h"
#include "wbase/Base.h"
#include "wconfig/WorkerConfig.h"
#include "wconfig/WorkerConfigError.h"
#include "wcontrol/Foreman.h"
#include "wpublish/ChunkInventory.h"
#include "wsched/BlendScheduler.h"
#include "wsched/FifoScheduler.h"
#include "wsched/GroupScheduler.h"
#include "wsched/ScanScheduler.h"
#include "xrdsvc/SsiSession.h"
#include "xrdsvc/XrdName.h"


class XrdPosixCallBack; // Forward.

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.xrdsvc.SsiService");

// add LWP to MDC in log messages
void initMDC() {
    LOG_MDC("LWP", std::to_string(lsst::log::lwpID()));
}
int dummyInitMDC = LOG_MDC_INIT(initMDC);

}

namespace lsst {
namespace qserv {
namespace xrdsvc {

SsiService::SsiService(XrdSsiLogger* log, wconfig::WorkerConfig const& workerConfig)
    : _mySqlConfig(workerConfig.getMySqlConfig()) {
    LOGS(_log, LOG_LVL_DEBUG, "SsiService starting...");


    if (not _mySqlConfig.checkConnection()) {
    LOGS(_log, LOG_LVL_FATAL, "Unable to connect to MySQL using configuration:" << _mySqlConfig);
    throw wconfig::WorkerConfigError("Unable to connect to MySQL");
    }
    _initInventory();

    std::string cfgMemMan = workerConfig.getMemManClass();
    memman::MemMan::Ptr memMan;
    if (cfgMemMan  == "MemManReal") {
        // Default to 1 gigabyte
        uint64_t memManSize = workerConfig.getMemManSizeMb()*1000000;
		std::string memManLocation = workerConfig.getMemManLocation();
        LOGS(_log, LOG_LVL_DEBUG, "Using MemManReal with memManSizeMb=" << memManSize
                << " location=" << memManLocation);
        memMan = std::shared_ptr<memman::MemMan>(memman::MemMan::create(memManSize, memManLocation));
    } else if (cfgMemMan == "MemManNone"){
        memMan = std::make_shared<memman::MemManNone>(1, false);
    } else {
        LOGS(_log, LOG_LVL_ERROR, "Unrecognized memory manager " << cfgMemMan);
        throw wconfig::WorkerConfigError("Unrecognized memory manager.");
    }

    // Set thread pool size.
    uint poolSize = std::max(workerConfig.getThreadPoolSize(), std::thread::hardware_concurrency());

    // poolSize should be greater than either GroupScheduler::maxThreads or ScanScheduler::maxThreads
    uint maxThread = poolSize;
    int maxReserve = 2;
    auto group = std::make_shared<wsched::GroupScheduler>(
                 "SchedGroup", maxThread, maxReserve, workerConfig.getMaxGroupSize(), wsched::SchedulerBase::getMaxPriority());

    int const fastest = lsst::qserv::proto::ScanInfo::Rating::FASTEST;
    int const fast    = lsst::qserv::proto::ScanInfo::Rating::FAST;
    int const medium  = lsst::qserv::proto::ScanInfo::Rating::MEDIUM;
    int const slow    = lsst::qserv::proto::ScanInfo::Rating::SLOW;
    std::vector<wsched::ScanScheduler::Ptr> scanSchedulers{
        std::make_shared<wsched::ScanScheduler>(
                 "SchedSlow", maxThread, workerConfig.getMaxReserveSlow(), workerConfig.getPrioritySlow(), memMan, medium+1, slow),
        std::make_shared<wsched::ScanScheduler>(
                 "SchedMed", maxThread, workerConfig.getMaxReserveMed(), workerConfig.getPriorityMed(), memMan, fast+1, medium),
        std::make_shared<wsched::ScanScheduler>(
                 "SchedFast", maxThread, workerConfig.getMaxReserveFast(), workerConfig.getPriorityFast(), memMan, fastest, fast)
    };

    _foreman = std::make_shared<wcontrol::Foreman>(
        std::make_shared<wsched::BlendScheduler>("BlendSched", maxThread, group, scanSchedulers),
        poolSize,
        workerConfig.getMySqlConfig());
}

SsiService::~SsiService() {
    LOGS(_log, LOG_LVL_DEBUG, "SsiService dying.");
}

void SsiService::Provision(XrdSsiService::Resource* r,
                           unsigned short timeOut,
                           bool userConn) { // Step 2
    LOGS(_log, LOG_LVL_DEBUG, "Got provision call where rName is: " << r->rName);
    XrdSsiSession* session = new SsiSession(r->rName, _chunkInventory->newValidator(), _foreman);
    r->ProvisionDone(session); // Step 3: trigger client-side ProvisionDone()
}

void SsiService::_initInventory() {
    XrdName x;
    if (not _mySqlConfig.dbName.empty()) {
            LOGS(_log, LOG_LVL_FATAL, "dbName must be empty to prevent accidental context");
            throw std::runtime_error("dbName must be empty to prevent accidental context");

    }
    std::shared_ptr<sql::SqlConnection> conn = std::make_shared<sql::SqlConnection>(_mySqlConfig, true);
    assert(conn);
    _chunkInventory = std::make_shared<wpublish::ChunkInventory>(x.getName(), conn);
    std::ostringstream os;
    os << "Paths exported: ";
    _chunkInventory->dbgPrint(os);
    LOGS(_log, LOG_LVL_DEBUG, os.str());
}


}}} // namespace
