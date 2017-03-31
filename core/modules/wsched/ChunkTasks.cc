// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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

// System headers
#include <vector>

// Class header
#include "ChunkTasks.h"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "wbase/Task.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wsched.ChunkTasksQueue");
}

namespace lsst {
namespace qserv {
namespace wsched {

/// Remove task from ChunkTasks.
/// This depends on owner for thread safety.
/// @return a pointer to the removed task or
wbase::Task::Ptr ChunkTasks::removeTask(wbase::Task::Ptr const& task) {
    auto eraseFunc = [&task](std::vector<wbase::Task::Ptr> &vect)->wbase::Task::Ptr {
        auto queryId = task->getQueryId();
        auto jobId = task->getJobId();
        for (auto iter = vect.begin(); iter != vect.end(); ++iter) {
            if ((*iter)->idsMatch(queryId, jobId)) {
                auto ret = *iter;
                vect.erase(iter);
                return ret;
            }
        }
        return nullptr;
    };

    wbase::Task::Ptr result = nullptr;
    // Is it in _activeTasks?
    result = eraseFunc(_activeTasks._tasks);
    if (result != nullptr) {
        _activeTasks.heapify();
        return result;
    }

    // Is it in _pendingTasks?
    return eraseFunc(_pendingTasks);
}


/// Queue new Tasks to be run, ordered with the slowest tables first.
/// This relies on ChunkTasks owner for thread safety.
void ChunkTasks::queTask(wbase::Task::Ptr const& a) {
    time(&a->entryTime);
    /// Compute entry time to reduce spurious valgrind errors
    ::ctime_r(&a->entryTime, a->timestr);

    const char* state = "";
    // If this is the active chunk, put new Tasks on the pending list, as
    // we could easily get stuck on this chunk as new Tasks come in.
    if (_active) {
        _pendingTasks.push_back(a);
        state = "PENDING";
    } else {
        _activeTasks.push(a);
        state = "ACTIVE";
    }
    LOGS(_log, LOG_LVL_DEBUG,
         "ChunkTasks queue "
         << a->getIdStr()
         << " chunkId=" << _chunkId
         << " state=" << state
         << " active.sz=" << _activeTasks._tasks.size()
         << " pend.sz=" << _pendingTasks.size());
    if (_activeTasks.empty()) {
        LOGS(_log, LOG_LVL_DEBUG, "Top of ACTIVE is now: (empty)");
    } else {
        LOGS(_log, LOG_LVL_DEBUG, "Top of ACTIVE is now: " << _activeTasks.top()->getIdStr());
    }
}


/// Set this chunk as the active chunk and move pending jobs to active if needed.
void ChunkTasks::setActive(bool active) {
    if (_active != active) {
        LOGS(_log, LOG_LVL_DEBUG, "ChunkTasks " << _chunkId << " active changed to " << active);
        if (_active && !active) {
            movePendingToActive();
        }
    }
    _active = active;
}


/// Move all pending Tasks to the active heap.
void ChunkTasks::movePendingToActive() {
    for (auto const& t:_pendingTasks) {
        LOGS(_log, LOG_LVL_DEBUG, "ChunkTasks " << _chunkId << " pending->active " << t->getIdStr());
        _activeTasks.push(t);
    }
    _pendingTasks.clear();
}


/// @return true if active AND pending are empty.
bool ChunkTasks::empty() const {
    return _activeTasks.empty() && _pendingTasks.empty();
}


/// This is ready to advance when _activeTasks is empty and no Tasks are in flight.
bool ChunkTasks::readyToAdvance() {
    return _activeTasks.empty() && _inFlightTasks.empty();
}


/// @Return true if a Task is ready to be run.
// ChunkTasks does not have its own mutex and depends on its owner for thread safety.
// If a Task is ready to be run, _readyTask will not be nullptr.
ChunkTasks::ReadyState ChunkTasks::ready(bool useFlexibleLock) {
    auto logMemManRes =
            [this, useFlexibleLock](bool starved, std::string const& msg, int handle,
                   std::vector<memman::TableInfo> const& tblVect) {
        setResourceStarved(starved);
        if (!starved) {
            std::string str;
            for (auto const& tblInfo:tblVect) {
                str += tblInfo.tableName + " ";
            }
            LOGS(_log, LOG_LVL_DEBUG, "ready memMan flex=" << useFlexibleLock
                  << " handle=" << handle << " " << msg << " - " << str);
        }
    };

    if (_readyTask != nullptr) {
        return ChunkTasks::ReadyState::READY;
    }
    if (_activeTasks.empty()) {
        return ChunkTasks::ReadyState::NOT_READY;
    }

    // Calling this function doesn't get expensive until it gets here. Luckily,
    // after this point it will return READY or NO_RESOURCES, and ChunkTasksQueue::_ready
    // will not examine any further chunks upon seeing those results.
    auto task = _activeTasks.top();
    if (!task->hasMemHandle()) {
        memman::TableInfo::LockType lckOptTbl = memman::TableInfo::LockType::REQUIRED;
        if (useFlexibleLock) lckOptTbl = memman::TableInfo::LockType::FLEXIBLE;
        memman::TableInfo::LockType lckOptIdx = memman::TableInfo::LockType::NOLOCK;
        auto scanInfo = task->getScanInfo();
        auto chunkId = task->getChunkId();
        if (chunkId != _chunkId) {
            // This would slow things down badly, but the system would survive.
            LOGS(_log, LOG_LVL_ERROR, "ChunkTasks " << _chunkId << " got task for chunk " << chunkId
                    << " " << task->getIdStr());
        }
        std::vector<memman::TableInfo> tblVect;
        for (auto const& tbl : scanInfo.infoTables) {
            memman::TableInfo ti(tbl.db + "/" + tbl.table, lckOptTbl, lckOptIdx);
            tblVect.push_back(ti);
        }
        // If tblVect is empty, we should get the empty handle
        memman::MemMan::Handle handle = _memMan->prepare(tblVect, chunkId);
        if (handle == 0) {
            switch (errno) {
            case ENOMEM:
                logMemManRes(true, "ENOMEM", handle, tblVect);
                return ChunkTasks::ReadyState::NO_RESOURCES;
            case ENOENT:
                LOGS(_log, LOG_LVL_ERROR, "_memMgr->lock errno=ENOENT chunk not found " << task->getIdStr());
                // Not sure if this is the best course of action, but it should just need one
                // logic path. The query should fail from the missing tables
                // and the czar must be able to handle that with appropriate retries.
                handle = memman::MemMan::HandleType::ISEMPTY;
                break;
            default:
                LOGS(_log, LOG_LVL_ERROR, "_memMgr->lock file system error " << task->getIdStr());
                // Any error reading the file system is probably fatal for the worker.
                throw std::bad_exception();
                return ChunkTasks::ReadyState::NO_RESOURCES;
            }
        }
        task->setMemHandle(handle);
        logMemManRes(false, task->getIdStr() + " got handle", handle, tblVect);
    }

    // There is a Task to run at this point, pull it off the heap to avoid confusion.
    _activeTasks.pop();
    _readyTask = task;
    return ChunkTasks::ReadyState::READY;
}


/// @return old value of _resourceStarved.
bool ChunkTasks::setResourceStarved(bool starved){
    auto val = _resourceStarved;
    _resourceStarved = starved;
    return val;
}



/// @return a Task that is ready to run, if available. Otherwise return nullptr.
/// ChunkTasks relies on its owner for thread safety.
wbase::Task::Ptr ChunkTasks::getTask(bool useFlexibleLock) {
    if (ready(useFlexibleLock) != ReadyState::READY) {
        LOGS(_log, LOG_LVL_DEBUG, "ChunkTasks " << _chunkId << " denying task");
        return nullptr;
    }
    // Return and clear _readyTask so it isn't called more than once.
    auto task = _readyTask;
    _readyTask = nullptr;
    if (task->getChunkId() == _chunkId) {
        _inFlightTasks.insert(task.get());
    }
    return task;
}


void ChunkTasks::taskComplete(wbase::Task::Ptr const& task) {
    _inFlightTasks.erase(task.get());
}

}}} // namespace lsst::qserv::wsched
