/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica_core/VerifyJob.h"

// System headers

#include <stdexcept>

// Qserv headers

#include "lsst/log/Log.h"
#include "replica_core/BlockPost.h"
#include "replica_core/ServiceProvider.h"

// This macro to appear witin each block which requires thread safety

#define LOCK_GUARD \
std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica_core.VerifyJob");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica_core {

///////////////////////////////////////////////////
///                ReplicaDiff                  ///
///////////////////////////////////////////////////

ReplicaDiff::ReplicaDiff ()
    :   _notEqual(false) {
}

ReplicaDiff::ReplicaDiff (ReplicaInfo const& replica1,
                          ReplicaInfo const& replica2)
    :   _replica1          (replica1),
        _replica2          (replica2),
        _notEqual          (false),
        _statusMismatch    (false),
        _numFilesMismatch  (false),
        _fileNamesMismatch (false),
        _fileSizeMismatch  (false),
        _fileCsMismatch    (false),
        _fileMtimeMismatch (false) {

    if ((replica1.database() != replica2.database()) ||
        (replica1.chunk   () != replica2.chunk   ()))
        throw std::invalid_argument ("ReplicaDiff::ReplicaDiff(r1,r2)  incompatible aruments");

    // Status and the number of files are expeted to match

    _statusMismatch   = replica1.status()          != replica2.status();
    _numFilesMismatch = replica1.fileInfo().size() != replica2.fileInfo().size();

    // Corresponding file entries must match

    std::map<std::string,ReplicaInfo::FileInfo> file2info1 = replica1.fileInfoMap();
    std::map<std::string,ReplicaInfo::FileInfo> file2info2 = replica2.fileInfoMap();

    for (auto const& f: file2info1) {

        // Check if each file is present in both collections
        std::string const& name = f.first;

        // The file name is required to be present in both replicas
        if (not file2info2.count(name)) {
            _fileNamesMismatch = true;
            continue;
        }
 
        ReplicaInfo::FileInfo const& file1 = file2info1[name];
        ReplicaInfo::FileInfo const& file2 = file2info2[name];

        _fileSizeMismatch = _fileSizeMismatch ||
            (file1.size != file2.size);

        // Control sums are considered only if they're both defined
        _fileCsMismatch = _fileCsMismatch ||
            ((not file1.cs.empty() and not file2.cs.empty()) && (file1.cs != file2.cs));

        _fileMtimeMismatch = _fileMtimeMismatch ||
            (file1.mtime != file2.mtime);
    }
    _notEqual =
        _statusMismatch ||
        _numFilesMismatch ||
        _fileNamesMismatch ||
        _fileSizeMismatch ||
        _fileCsMismatch ||
        _fileMtimeMismatch;
}

ReplicaDiff::ReplicaDiff (ReplicaDiff const& rhs)
    :   _replica1          (rhs._replica1),
        _replica2          (rhs._replica2),
        _notEqual          (rhs._notEqual),
        _statusMismatch    (rhs._statusMismatch),
        _numFilesMismatch  (rhs._numFilesMismatch),
        _fileNamesMismatch (rhs._fileNamesMismatch),
        _fileSizeMismatch  (rhs._fileSizeMismatch),
        _fileCsMismatch    (rhs._fileCsMismatch),
        _fileMtimeMismatch (rhs._fileMtimeMismatch) {
}

ReplicaDiff&
ReplicaDiff::operator= (ReplicaDiff const& rhs) {
    if (&rhs != this) {
        _replica1           = rhs._replica1;
        _replica2           = rhs._replica2;
        _notEqual           = rhs._notEqual;
        _statusMismatch     = rhs._statusMismatch;
        _numFilesMismatch   = rhs._numFilesMismatch;
        _fileNamesMismatch  = rhs._fileNamesMismatch;
        _fileSizeMismatch   = rhs._fileSizeMismatch;
        _fileCsMismatch     = rhs._fileCsMismatch;
        _fileMtimeMismatch  = rhs._fileMtimeMismatch;
    }
    return *this;
}

std::ostream& operator<< (std::ostream& os, ReplicaDiff const& ri) {
    ReplicaInfo const& r1 = ri.replica1();
    ReplicaInfo const& r2 = ri.replica2();
    os  << "ReplicaDiff {\n"
        << "  replica1\n"
        << "    worker:   " << r1.worker  () << "\n"
        << "    database: " << r1.database() << "\n"
        << "    chunk:    " << r1.chunk   () << "\n"
        << "    status:   " << ReplicaInfo::status2string (r1.status()) << "\n"
        << "  replica2\n"
        << "    worker:   " << r2.worker  () << "\n"
        << "    database: " << r2.database() << "\n"
        << "    chunk:    " << r2.chunk   () << "\n"
        << "    status:   " << ReplicaInfo::status2string (r2.status()) << "\n"
        << "  notEqual:            " << (ri()                    ? "true" : "false") << "\n"
        << "    statusMismatch:    " << (ri.statusMismatch    () ? "true" : "false") << "\n"
        << "    numFilesMismatch:  " << (ri.numFilesMismatch  () ? "true" : "false") << "\n"
        << "    fileNamesMismatch: " << (ri.fileNamesMismatch () ? "true" : "false") << "\n"
        << "    fileSizeMismatch:  " << (ri.fileSizeMismatch  () ? "true" : "false") << "\n"
        << "    fileCsMismatch:    " << (ri.fileCsMismatch    () ? "true" : "false") << "\n"
        << "    fileMtimeMismatch: " << (ri.fileMtimeMismatch () ? "true" : "false") << "}\n";
    return os;
}

/////////////////////////////////////////////////
///                VerifyJob                  ///
/////////////////////////////////////////////////


VerifyJob::pointer
VerifyJob::create (Controller::pointer const& controller,
                   callback_type              onFinish,
                   callback_type_on_diff      onReplicaDifference,
                   int                        priority,
                   bool                       exclusive,
                   bool                       preemptable) {
    return VerifyJob::pointer (
        new VerifyJob (controller,
                       onFinish,
                       onReplicaDifference,
                       priority,
                       exclusive,
                       preemptable));
}

VerifyJob::VerifyJob (Controller::pointer const& controller,
                      callback_type              onFinish,
                      callback_type_on_diff      onReplicaDifference,
                      int                        priority,
                      bool                       exclusive,
                      bool                       preemptable)

    :   Job (controller,
             "VERIFY",
             priority,
             exclusive,
             preemptable),

        _onFinish            (onFinish),
        _onReplicaDifference (onReplicaDifference) {
}

VerifyJob::~VerifyJob () {
}

void
VerifyJob::track (bool          progressReport,
                  bool          errorReport,
                  bool          chunkLocksReport,
                  std::ostream& os) const {

    if (_state == State::FINISHED) return;
    
    BlockPost blockPost (1000, 2000);

    while (_state != State::FINISHED) {
        blockPost.wait();
        if (progressReport)
            os  << "VerifyJob::track()  "
                << "replica: " << _replica
                << std::endl;
    }
}

void
VerifyJob::startImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "startImpl");

    auto self = shared_from_base<VerifyJob>();
    
    // In theory this should never happen

    if (not nextReplica()) {
        setState(State::FINISHED);
        return;
    }

    // Launch the very first request
    _request = _controller->findReplica (
        _replica.worker   (),
        _replica.database (),
        _replica.chunk    (),
        [self] (FindRequest::pointer request) {
            self->onRequestFinish (request);
        },
        0,      /* priority */
        true,   /* computeCheckSum */
        true,   /* keepTracking*/
        _id     /* jobId */
    );

    setState(State::IN_PROGRESS);
}

void
VerifyJob::cancelImpl () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "cancelImpl");

    // To ensure no lingering "side effects" will be left after cancelling this
    // job the request cancellation should be also followed (where it makes a sense)
    // by stopping the request at corresponding worker service.

    if (_request) {
        _request->cancel();
        if (_request->state() != Request::State::FINISHED)
            _controller->stopReplicaFind (
                _request->worker(),
                _request->id(),
                nullptr,    /* onFinish */
                true,       /* keepTracking */
                _id         /* jobId */);
    }
    _request = nullptr;

    setState(State::FINISHED, ExtendedState::CANCELLED);
}

void
VerifyJob::notify () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    if (_onFinish) {
        auto self = shared_from_base<VerifyJob>();
        _onFinish(self);
    }
}

void
VerifyJob::onRequestFinish (FindRequest::pointer request) {

    LOGS(_log, LOG_LVL_DEBUG, context()
         << "onRequestFinish  database=" << request->database()
         << " worker=" << request->worker()
         << " chunk="  << request->chunk());

    // Ignore the callback if the job was cancelled   
    if (_state == State::FINISHED) return;

    auto self = shared_from_base<VerifyJob>();

    // The default version of the object won't have any difference
    // reported
    ReplicaDiff replicaDiff;
    {
        LOCK_GUARD;

        if (request->extendedState() == Request::ExtendedState::SUCCESS) {
            
            // TODO:
            // - check if the replica still exists. It's fine if it's gone
            //   because some jobs may chhose either to purge extra replicas
            //   or rebalance the cluster. So, no subscriber otification is needed
            //   here.

            ;
            
            // Compare new state of the replica against its older one which was
            // known to the database before this request was launched. Notify
            // a subscriber of any changes (after releasing LOCK_GUARD). Changes
            // which should be tracked include:
            // - the status of the replica (unless it's NOT_FOUND as discussed above)
            // - the number of files
            // - the names of files
            // - file sizes
            // - mtime of files
            // - constrol/check sums of files (if they were known before)

            replicaDiff = ReplicaDiff (_replica, request->responseData());
            if (replicaDiff())
                LOGS(_log, LOG_LVL_ERROR, context() << "replica missmatch\n" << replicaDiff);

        } else {

            // TODO: should we just report the error and launch another
            // request?
            ;
        }
        if (nextReplica ()) {

            _request = _controller->findReplica (
                _replica.worker   (),
                _replica.database (),
                _replica.chunk    (),
                [self] (FindRequest::pointer request) {
                    self->onRequestFinish (request);
                },
                0,      /* priority */
                true,   /* computeCheckSum */
                true,   /* keepTracking*/
                _id     /* jobId */
            );

        } else {

            // In theory this should never happen unless all replicas are gone
            // from the system or there was a problem to access the database.

            setState (State::FINISHED);
        }
    }

    // NOTE: The callbacks are called w/o keeping a lock on th eobject API
    // to prevent potential deadlocks.

    if (replicaDiff() && _onReplicaDifference)
        _onReplicaDifference (self, replicaDiff);
    
    if (_state == State::FINISHED) notify ();
}

bool
VerifyJob::nextReplica () {
    return _controller->serviceProvider().databaseServices()->findOldestReplica(_replica);
}

}}} // namespace lsst::qserv::replica_core