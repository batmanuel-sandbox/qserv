// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 AURA/LSST.
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
#include "loader/MasterServer.h"

// System headers
#include <iostream>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.MasterServer");
}

namespace lsst {
namespace qserv {
namespace loader {

BufferUdp::Ptr MasterServer::parseMsg(BufferUdp::Ptr const& data, udp::endpoint const& senderEndpoint) {
    // echo server, so send back what we got
    BufferUdp::Ptr sendData = data;
    std::string str(sendData->begin(), sendData->getCurrentLength());
    std::cout << "str len=" << str.length() << std::endl;
    LOGS(_log, LOG_LVL_INFO, "Master bytes(" << sendData->getCurrentLength() << "):(" << str <<
            ") from endpoint " << senderEndpoint);

    return sendData;
}


}}} // namespace lsst:qserv::loader


