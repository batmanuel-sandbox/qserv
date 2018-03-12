// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2018 LSST Corporation.
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
 *  @author: John Gates,
 */

// Class header
#include "util/ThreadPool.h"

// System headers
#include <algorithm>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.util.ThreadPool");
}

namespace lsst {
namespace qserv {
namespace util {

/// PoolEventThread factory to ensure shared_this.
PoolEventThread::Ptr PoolEventThread::newPoolEventThread(std::shared_ptr<ThreadPool> const& threadPool,
                                                         CommandQueue::Ptr const& q) {
    PoolEventThread::Ptr pet(new PoolEventThread(threadPool, q));
    return pet;
}


PoolEventThread::PoolEventThread(std::shared_ptr<ThreadPool> const& threadPool, CommandQueue::Ptr const& q)
: EventThread(q), _threadPool(threadPool) {
    LOGS(_log, LOG_LVL_DEBUG, "PoolEventThread::PoolEventThread() " << this);
}


PoolEventThread::~PoolEventThread() {
    LOGS(_log, LOG_LVL_DEBUG, "PoolEventThread::~PoolEventThread() " << this
         << " _threadPool.use_count=" << _threadPool.use_count());
}


/// If cmd is a CommandThreadPool object, give it a copy of our this pointer.
void PoolEventThread::specialActions(Command::Ptr const& cmd) {
    CommandThreadPool::Ptr cmdPool = std::dynamic_pointer_cast<CommandThreadPool>(cmd);
    if (cmdPool != nullptr) {
        cmdPool->_setPoolEventThread(shared_from_this());
    }
}


/// Cause this thread to leave the thread pool, this can be called from outside of the
// thread that will be removed from the pool. This would most likely be done by
// a CommandQueue which was having some trouble due to 'cmd', such as 'cmd' taking
// too long to complete. This allows the CommandQueue to continue but will have other
// consequences.
// @return false if a different command is running than cmd.
bool PoolEventThread::leavePool(Command::Ptr const& cmd) {
    // This thread will stop accepting commands
    _loop = false;
    LOGS(_log, LOG_LVL_DEBUG, "PoolEventThread::leavePool " << this);
    if (cmd.get() != getCurrentCommand()) {
        LOGS(_log, LOG_LVL_DEBUG, "PoolEventThread::leavePool different command" << this);
        // cmd must have finished before the event loop stopped.
        // The current command will complete normally, and the pool
        // should replace this thread with a new one when finishup()
        // is called in handleCmds(). No harm aside from some wasted CPU cycles.
        return false;
    }

    // Have the CommandQueue deal with any accounting that needs to be done.
    callCommandFinish(cmd);

    // Have the thread pool release this thread, which will cause a replacement thread
    // to be created.
    finishup();
    return true;
}


/// Cause this thread to leave the thread pool, this MUST only called from within
// the thread that will be removed (most likely from within a CommandThreadPool action).
void PoolEventThread::leavePool() {
    // This thread will stop accepting commands
    _loop = false;
    leavePool(_getCurrentCommandPtr());
}


void PoolEventThread::finishup() {
    if (_finishupOnce.exchange(true) == false) {
        LOGS(_log, LOG_LVL_DEBUG, "Releasing this PoolEventThread");
        if (!_threadPool->release(this)) {
            LOGS(_log, LOG_LVL_WARN, "The pool failed to find this PoolEventThread.");
        }
    }
}


/// Set _poolEventThread pointer to the thread running this command.
void CommandThreadPool::_setPoolEventThread(PoolEventThread::Ptr const& poolEventThread) {
    _poolEventThread = poolEventThread;
}


/// Invalidate _poolEventThread so it can't be used again.
/// At this point, the reason to get _poolEventThread is to
/// have the thread leave the pool. This prevents that from
/// happening more than once.
PoolEventThread::Ptr CommandThreadPool::getAndNullPoolEventThread() {
    auto pet = _poolEventThread.lock();
    _poolEventThread.reset();
    return pet;
}


ThreadPool::Ptr ThreadPool::newThreadPool(unsigned int thrdCount,
      CommandQueue::Ptr const& q, EventThreadJoiner::Ptr const& joiner) {
    Ptr thp(new ThreadPool(thrdCount, q, joiner)); // private constructor
    thp->_resize();
    return thp;
}


ThreadPool::ThreadPool(unsigned int thrdCount, CommandQueue::Ptr const& q,
                       EventThreadJoiner::Ptr const& joiner)
    : _targetThrdCount(thrdCount), _q(q), _joinerThread(joiner) {
    if (_q == nullptr) {
        _q = std::make_shared<CommandQueue>();
    }
    if (_joinerThread == nullptr) {
        _joinerThread = std::make_shared<EventThreadJoiner>();
    }
}


ThreadPool::~ThreadPool() {
    if (!_shutdown) {
        LOGS(_log, LOG_LVL_WARN, "~ThreadPool called without shutdownPool being called first.");
    }
    LOGS(_log, LOG_LVL_DEBUG, "~ThreadPool " << this);
}


/// Wait for all threads to complete. The ThreadPool should not be used after this function is called.
/// This includes threads that were removed from the pool and not detached.
void ThreadPool::shutdownPool() {
    LOGS(_log, LOG_LVL_DEBUG,"shutdownPool begin" << this);
    _shutdown = true;
    endAll();
    waitForResize(0);
    _joinerThread->shutdownJoin();
}

/// Release the thread from the thread pool and return a shared pointer to the
/// released thread.
bool ThreadPool::release(PoolEventThread *thrd) {
    // Search for the the thread to free
    auto func = [thrd](PoolEventThread::Ptr const& pt)->bool {
        return pt.get() == thrd;
    };

    PoolEventThread::Ptr thrdPtr;
    {
        std::lock_guard<std::mutex> lock(_poolMutex);
        auto iter = std::find_if(_pool.begin(), _pool.end(), func);
        if (iter == _pool.end()) {
            LOGS(_log, LOG_LVL_WARN, "ThreadPool::release thread not found " << thrd);
            return false;
        } else {
            thrdPtr = *iter;
            LOGS(_log, LOG_LVL_DEBUG, "ThreadPool::release erasing " << thrd);
            _pool.erase(iter);
        }
        _joinerThread->addThread(thrdPtr); // Add to list of threads to join.
    }
    _resize(); // Check if more threads need to be released.
    return true;
}

/// Change the size of the thread pool.
void ThreadPool::resize(unsigned int targetThrdCount) {
    {
        std::lock_guard<std::mutex> lock(_countMutex);
        if (_shutdown) {
            targetThrdCount = 0;
        }
        _targetThrdCount = targetThrdCount;
    }
    _resize();
}

/// Do the work of changing the size of the thread pool.
/// Making the pool larger is just a matter of adding threads.
/// Shrinking the pool requires ending one thread at a time.
void ThreadPool::_resize() {
    std::lock_guard<std::mutex> lock(_poolMutex);
    auto target = getTargetThrdCount();
    while (target > _pool.size()) {
        LOGS(_log, LOG_LVL_DEBUG, "ThreadPool::_resize creating new PoolEventThread");
        auto t = PoolEventThread::newPoolEventThread(shared_from_this(), _q);
        _pool.push_back(t);
        t->run();
    }
    // Shrinking the thread pool is much harder. Adding a message to end one thread
    // is sent. When that thread ends, it calls release(), which will then call
    // this function again to check if more threads need to be ended.
    if (target < _pool.size()) {
        auto thrd = _pool.front();
        if (thrd != nullptr) {
            LOGS(_log, LOG_LVL_DEBUG, "ThreadPool::_resize sending thrd->queEnd()");
            thrd->queEnd(); // Since all threads share the same queue, this could be answered by any thread.
        } else {
            LOGS(_log, LOG_LVL_WARN, "ThreadPool::_resize thrd == nullptr");
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, "_resize target=" << target << " size=" << _pool.size());
    {
        std::unique_lock<std::mutex> countlock(_countMutex);
        _countCV.notify_all();
    }
}

/// Wait for the pool to reach the _targetThrdCount number of threads.
/// It will wait forever if 'millisecs' is zero, otherwise it will timeout
/// after that number of milliseconds.
/// Note that this wont detect changes to _targetThrdCount.
void ThreadPool::waitForResize(int millisecs) {
    auto eqTest = [this](){ return _targetThrdCount == _pool.size(); };
    std::unique_lock<std::mutex> lock(_countMutex);
    if (millisecs > 0) {
        _countCV.wait_for(lock, std::chrono::milliseconds(millisecs), eqTest);
    } else {
        _countCV.wait(lock, eqTest);
    }
}

}}} // namespace lsst:qserv:util
