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
 */
#ifndef LSST_QSERV_REPLICA_STATUS_REQUEST_BASE_H
#define LSST_QSERV_REPLICA_STATUS_REQUEST_BASE_H

/// StatusRequestBase.h declares:
///
/// Common classes shared by all implementations:
///
///   class StatusRequestBase
///
/// (see individual class documentation for more information)

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Common.h"
#include "replica/Messenger.h"
#include "replica/RequestMessenger.h"
#include "replica/ServiceProvider.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class StatusRequestBase represents the base class for a family of requests
  * pulling a status of on-going operationd.
  */
class StatusRequestBase
    :   public RequestMessenger {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<StatusRequestBase> Ptr;

    // Default construction and copy semantics are prohibited

    StatusRequestBase() = delete;
    StatusRequestBase(StatusRequestBase const&) = delete;
    StatusRequestBase& operator=(StatusRequestBase const&) = delete;

    /// Destructor
    ~StatusRequestBase() override = default;

    /// Return an identifier of the target request
    std::string const& targetRequestId() const { return _targetRequestId; }

    /// Return the performance info of the target operation (if available)
    Performance const& targetPerformance() const { return _targetPerformance; }

protected:

    /**
     * Construct the request with the pointer to the services provider.
     */
    StatusRequestBase(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      char const* requestTypeName,
                      std::string const& worker,
                      std::string const& targetRequestId,
                      proto::ReplicationReplicaRequestType requestType,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

    /**
      * Implement the method declared in the base class
      *
      * @see Request::startImpl()
      */
    void startImpl() final;

    /// Start the timer before attempting the previously failed
    /// or successfull (if a status check is needed) step.
    void wait();

    /// Callback handler for the asynchronious operation
    void awaken(boost::system::error_code const& ec);

    /**
     * Initiate request-specific send
     *
     * This method must be implemented by subclasses.
     */
    virtual void send() = 0;

    /**
     * Process the worker response to the requested operation.
     *
     * @param success - the flag indicating if the operation was successfull
     * @param status  - a response from the worker service (only valid if success is 'true')
     */
    void analyze(bool success,
                 proto::ReplicationStatus status = proto::ReplicationStatus::FAILED);

     /**
      * Initiate request-specific operation with the persistent state
      * service to store replica status.
      *
      * This method must be implemented by subclasses.
      */
     virtual void saveReplicaInfo() = 0;

private:

    /// An identifier of the targer request whose state is to be queried
    std::string _targetRequestId;

    /// The type of the targer request (must match the identifier)
    proto::ReplicationReplicaRequestType  _requestType;

protected:

    /// The performance of the target operation
    Performance _targetPerformance;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_STATUS_REQUEST_BASE_H