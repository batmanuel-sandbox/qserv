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
#ifndef LSST_QSERV_REPLICA_CORE_DATABASE_SERVICES_H
#define LSST_QSERV_REPLICA_CORE_DATABASE_SERVICES_H

/// DatabaseServices.h declares:
///
/// class DatabaseServices
/// (see individual class documentation for more information)

// System headers

#include <memory>
#include <mutex>
#include <string>
#include <vector>

// Qserv headers

#include "replica_core/Configuration.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

// Forward declarations

struct ControllerIdentity;

class Job;
class ReplicaInfo;
class Request;

/**
  * Class DatabaseServices is a high-level interface to the database services
  * for replication entities: Controller, Job and Request.
  *
  * This is also a base class for database technology-specific implementations
  * of the service. This particular class has dummy implementations of the
  * corresponding methods.
  */
class DatabaseServices
    :   public std::enable_shared_from_this<DatabaseServices> {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<DatabaseServices> pointer;

    /// Forward declaration for the smart reference to Job objects
    typedef std::shared_ptr<Job> Job_pointer;

    /// Forward declaration for the smart reference to Request objects
    typedef std::shared_ptr<Request> Request_pointer;

    /**
     * The factory method for instamtiating a proper service object based
     * on an application configuration.
     *
     * @param configuration - the configuration service
     */
    static pointer create (Configuration::pointer const& configuration);

    // Default construction and copy semantics are proxibited

    DatabaseServices () = delete;
    DatabaseServices (DatabaseServices const&) = delete;
    DatabaseServices& operator= (DatabaseServices const&) = delete;

    /// Destructor
    virtual ~DatabaseServices ();

    /**
     * Save the state of the Controller. Note this operation can be called
     * just once for a particular instance of the Controller.
     *
     * @param identity  - a data structure encapsulating a unique identity of
     *                    the Contriller instance.
     * @param startTime - a time (milliseconds since UNIX Epoch) when an instance of
     *                    the Controller was created.
     *
     * @throws std::logic_error - if this Contoller's state is already found in a database
     */
    virtual void saveState (ControllerIdentity const& identity,
                            uint64_t                  startTime);

    /**
     * Save the state of the Job. This operation can be called many times for
     * a particular instance of the Job.
     *
     * NOTE: The method will convert a pointer of the base class Job into
     * the final type to avoid type prolifiration through this interface.
     *
     * @param job - a pointer to a Job object
     *
     * @throw std::invalid_argument - if the actual job type won't match the expected one
     */
    virtual void saveState (Job_pointer const& job);

    /**
     * Save the state of the Request. This operation can be called many times for
     * a particular instance of the Request.
     *
     * NOTE: The method will convert a pointer of the base class Request into
     * the final type to avoid type prolifiration through this interface.
     *
     * @param request - a pointer to a Request object
     *
     * @throw std::invalid_argument - if the actual request type won't match the expected one
     */
    virtual void saveState (Request_pointer const& request);

    /**
     * Locate a replica which has the has the oldest verification timestamp.
     * Return 'true' and set the reference if the one is found.
     *
     * ATTENTION: no assumption on a new status of the replica object
     * passed into the method should be made if the operation fails
     * (returns 'false').
     *
     * @param replica - a reference to an object to be initialized
     */
    virtual bool findOldestReplica (ReplicaInfo& replica) const;
    
    /**
     * Find all replicas for the specified chunk and the database.
     *
     * ATTENTION: no assumption on a new status of the replica collection
     * passed into the method should be made if the operation fails
     * (returns 'false').
     *
     * @param replicas - a collection of replicas (if any found)
     * @param chunk    - the chunk number
     * @param database - the name of a database
     *
     * @return 'true' in case of success (even if no replicas were found)
     *
     * @throw std::invalid_argument - if the database is unknown or empty
     */
    virtual bool findReplicas (std::vector<ReplicaInfo>& replicas,
                               unsigned int              chunk,
                               std::string const&        database) const;

protected:

    /// Return shared pointer of the desired subclass (no dynamic type checking)
    template <class T>
    std::shared_ptr<T> shared_from_base () {
        return std::static_pointer_cast<T>(shared_from_this());
    }

    /**
     * Construct the object.
     *
     * @param configuration - the configuration service
     */
    explicit DatabaseServices (Configuration::pointer const& configuration);

protected:

    /// The configuration service
    Configuration::pointer _configuration;

    /// The mutex for enforcing thread safety of the class's public API
    /// and internal operations.
    mutable std::mutex _mtx;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_DATABASE_SERVICES_H