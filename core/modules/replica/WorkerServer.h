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
#ifndef LSST_QSERV_REPLICA_WORKER_SERVER_H
#define LSST_QSERV_REPLICA_WORKER_SERVER_H

/// WorkerServer.h declares:
///
/// class WorkerServer
/// (see individual class documentation for more information)

// System headers
#include <memory>

// Third party headers
#include <boost/asio.hpp>

// Qserv headers
#include "replica/ServiceProvider.h"
#include "replica/WorkerProcessor.h"
#include "replica/WorkerServerConnection.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class WorkerInfo;
class WorkerRequestFactory;

/**
  * Class WorkerServer is used for handling incomming connections to
  * the worker replication service. Only one instance of this class is
  * allowed per a thread.
  */
class WorkerServer
    : public std::enable_shared_from_this<WorkerServer>  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<WorkerServer> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider - for configuration, etc. services
     * @param requestFactory  - the factory of requests
     * @workerName            - the name of a worker this instance represents
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      WorkerRequestFactory& requestFactory,
                      std::string const& workerName);

    // Default construction and copy semantics are prohibited

    WorkerServer() = delete;
    WorkerServer(WorkerServer const&) = delete;
    WorkerServer& operator=(WorkerServer const&) = delete;

    /// Destructor
    ~WorkerServer() = default;

    /// Return the name of a worker this server runs for
    std::string const& worker() const { return _workerName; }

    /// The processor API can be used for detailed monitoring of
    /// the on-going activities and statistics collection if needed.
    WorkerProcessor const& processor() const { return _processor; }

    /**
     * Begin listening for and processing incoming connections
     */
    void run();

private:

    /**
     * Construct the server with the specified configuration.
     *
     * @param serviceProvider - for configuration, etc. services
     * @param requestFactory  - the factory of requests
     * @workerName            - the name of a worker this instance represents
     */
    WorkerServer(ServiceProvider::Ptr const& serviceProvider,
                 WorkerRequestFactory& requestFactory,
                 std::string const& workerName);

    /**
     * Begin (asynchrnonously) accepting connection requests.
     */
    void beginAccept();
    
    /**
     * Handle a connection request once it's detected. The rest of
     * the comunication will be forewarded to the connection object
     * specified as a parameter of the method.
     */
    void handleAccept(WorkerServerConnection::Ptr const& connection,
                      boost::system::error_code const& ec);

    /// Return the context string
    std::string context() const { return "SERVER  "; }

private:

    // Parameters of the object

    ServiceProvider::Ptr _serviceProvider;
    std::string _workerName;

    // Cached parameters of the worker

    WorkerProcessor _processor;

    WorkerInfo const& _workerInfo;

    // The mutable state of the object

    boost::asio::io_service        _io_service;
    boost::asio::ip::tcp::acceptor _acceptor;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKER_SERVER_H