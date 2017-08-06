// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_REPLICA_CORE_WORKERFINDREQUEST_H
#define LSST_QSERV_REPLICA_CORE_WORKERFINDREQUEST_H

/// WorkerFindRequest.h declares:
///
/// class WorkerFindRequest
/// (see individual class documentation for more information)

// System headers

#include <string>

// Qserv headers

#include "replica_core/WorkerRequest.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {


/**
  * Class WorkerFindRequest represents a context and a state of replica lookup
  * requsts within the worker servers. It can also be used for testing the framework
  * operation as its implementation won't make any changes to any files or databases.
  *
  * Real implementations of the request processing must derive from this class.
  */
class WorkerFindRequest
    :   public WorkerRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerFindRequest> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create (int                priority,
                           const std::string &id,
                           const std::string &database,
                           unsigned int       chunk);

    // Default construction and copy semantics are proxibited

    WorkerFindRequest () = delete;
    WorkerFindRequest (WorkerFindRequest const&) = delete;
    WorkerFindRequest & operator= (WorkerFindRequest const&) = delete;

    /// Destructor
    ~WorkerFindRequest () override;

    // Trivial accessors

    const std::string& database () const { return _database; }
    unsigned int       chunk    () const { return _chunk; }

private:

    /**
     * The normal constructor of the class.
     */
    WorkerFindRequest (int                priority,
                       const std::string &id,
                       const std::string &database,
                       unsigned int       chunk);
private:

    std::string  _database;
    unsigned int _chunk;
};

/**
  * Class WorkerReplicationRequestX provides an actual implementation for
  * the replica lookup requests using XRootD.
  */
class WorkerFindRequestX
    :   public WorkerFindRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerFindRequestX> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create (int                priority,
                           const std::string &id,
                           const std::string &database,
                           unsigned int       chunk);

    // Default construction and copy semantics are proxibited

    WorkerFindRequestX () = delete;
    WorkerFindRequestX (WorkerFindRequestX const&) = delete;
    WorkerFindRequestX & operator= (WorkerFindRequestX const&) = delete;

    /// Destructor
    ~WorkerFindRequestX () override;

    /**
     * This method implements the virtual method of the base class
     *
     * @see WorkerRequest::execute
     */
    bool execute (bool incremental=true) override;

private:

    /**
     * The normal constructor of the class.
     */
    WorkerFindRequestX (int                priority,
                        const std::string &id,
                        const std::string &database,
                        unsigned int       chunk);
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_WORKERFINDREQUEST_H