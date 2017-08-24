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

#include "replica_core/ServiceProvider.h"

// System headers

#include <algorithm>
#include <stdexcept>

// Qserv headers

#include "replica_core/Configuration.h"

namespace lsst {
namespace qserv {
namespace replica_core {


ServiceProvider::ServiceProvider (Configuration &configuration)
    :   _configuration(configuration) {
}

void
ServiceProvider::assertWorkerIsValid (const std::string &name) {
    if (!_configuration.isKnownWorker(name))
        throw std::invalid_argument (
            "Request::assertWorkerIsValid: worker name is not valid: " + name);
}

void
ServiceProvider::assertWorkersAreDifferent (const std::string &firstName,
                                            const std::string &secondName) {
    assertWorkerIsValid(firstName);
    assertWorkerIsValid(secondName);

    if (firstName == secondName)
        throw std::invalid_argument (
            "Request::assertWorkersAreDifferent: worker names are the same: " + firstName);
}

void
ServiceProvider::assertDatabaseIsValid (const std::string &name) {
    if (!_configuration.isKnownDatabase(name))
        throw std::invalid_argument (
            "Request::assertDatabaseIsValid: database name is not valid: " + name);
}


}}} // namespace lsst::qserv::replica_core