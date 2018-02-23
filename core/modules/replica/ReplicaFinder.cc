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

#include "replica/ReplicaFinder.h"

// System headers

#include <stdexcept>

// Qserv headers

#include "replica_core/Configuration.h"
#include "replica_core/ServiceProvider.h"

namespace lsst {
namespace qserv {
namespace replica {

ReplicaFinder::ReplicaFinder (replica_core::Controller::pointer const& controller,
                              std::string const&                       database,
                              std::ostream&                            os,
                              bool                                     progressReport,
                              bool                                     errorReport)
    :   replica_core::CommonRequestTracker<replica_core::FindAllRequest> (
            os,
            progressReport,
            errorReport) {

    // Launch requests against all workers

    for (const auto &worker: controller->serviceProvider().config().workers())
        add (
            controller->findAllReplicas (
                worker,
                database,
                [this] (replica_core::FindAllRequest::pointer ptr) {
                    this->onFinish(ptr);
                }
            )
        );

    // Wait before all request are finished. Then analyze results
    // and print a report on failed requests (if any)

    track();
}

ReplicaFinder::~ReplicaFinder () {
}

}}} // namespace lsst::qserv::replica
