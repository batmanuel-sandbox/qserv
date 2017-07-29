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
#ifndef LSST_QSERV_REPLICA_CORE_STOPREQUEST_H
#define LSST_QSERV_REPLICA_CORE_STOPREQUEST_H

/// StopRequest.h declares:
///
/// class StopRequest
/// (see individual class documentation for more information)

// System headers

#include <functional>   // std::function
#include <memory>       // shared_ptr
#include <string>

// Qserv headers

#include "proto/replication.pb.h"
#include "replica_core/Request.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica_core {

/**
  * Class StopRequest represents requests for stopping on-going replications.
  */
class StopRequest
    :   public Request  {

public:

    /// The only class which is allowed to instantiate and manage replications
    friend class MasterServer;

    /// The pointer type for instances of the class
    typedef std::shared_ptr<StopRequest> pointer;

    /// The function type for notifications on the completon of the request
    typedef std::function<void(pointer)> callback_type;

    // Default construction and copy semantics are proxibited

    StopRequest () = delete;
    StopRequest (StopRequest const&) = delete;
    StopRequest & operator= (StopRequest const&) = delete;

    /// Destructor
    ~StopRequest () final;

    /// Return an identifier of the target replication request
    const std::string& replicationRequestId () const {
        return _replicationRequestId;
    }

private:

    /**
     * Create a new request with specified parameters.
     * 
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider      - a host of services for various communications
     * @param worker               - the identifier of a worker node (the one to be affectd by the request)
     * @param io_service           - network communication service
     * @param replicationRequestId - a unique identifier of an existing replication request to be stopped
     * @param onFinish             - an optional callback function to be called upon a completion of
     *                               the request.
     */
    static pointer create (const ServiceProvider::pointer &serviceProvider,
                           const std::string              &worker,
                           boost::asio::io_service        &io_service,
                           const std::string              &replicationRequestId,
                           callback_type                   onFinish);

    /**
     * Construct the request with the pointer to the services provider.
     */
    StopRequest (const ServiceProvider::pointer &serviceProvider,
                 const std::string              &worker,
                 boost::asio::io_service        &io_service,
                 const std::string              &replicationRequestId,
                 callback_type                   onFinish);

    /**
      * This method is called when a connection is established and
      * the stack is ready to begin implementing an actual protocol
      * with the worker server.
      *
      * The first step of teh protocol will be to send the replication
      * request to the destination worker.
      */
    void beginProtocol () final;
    
    /// Callback handler for the asynchronious operation
    void requestSent (const boost::system::error_code &ec,
                      size_t bytes_transferred);

    /// Start receiving the response from the destination worker
    void receiveResponse ();

    /// Callback handler for the asynchronious operation
    void responseReceived (const boost::system::error_code &ec,
                           size_t bytes_transferred);

    /// Process the completion of the requested operation
    void analyze (lsst::qserv::proto::ReplicationStatus status);

    /**
     * Notifying a party which initiated the request.
     *
     * This method implements the corresponing virtual method defined
     * bu the base class.
     */
    void endProtocol () final;

private:

    // Parameters of the object

    std::string _replicationRequestId;

    // Registered callback to be called when the operation finishes

    callback_type _onFinish;
};

}}} // namespace lsst::qserv::replica_core

#endif // LSST_QSERV_REPLICA_CORE_STOPREQUEST_H