// -*- LSST-C++ -*-
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
 *
 */
#ifndef LSST_QSERV_LOADER_MASTERSERVER_H_
#define LSST_QSERV_LOADER_MASTERSERVER_H_

// system headers
#include <cstdlib>
#include <iostream>
#include <boost/bind.hpp>
#include <boost/asio.hpp>

// Qserv headers
#include "loader/ServerUdpBase.h"

namespace lsst {
namespace qserv {
namespace loader {

class MasterServer : public ServerUdpBase {
public:
    MasterServer(boost::asio::io_service& io_service, short port) : ServerUdpBase(io_service, port) {}

    ~MasterServer() override = default;

    BufferUdp::Ptr parseMsg(BufferUdp::Ptr const& data, udp::endpoint const& endpoint) override;
};


}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_MASTERSERVER_H_
