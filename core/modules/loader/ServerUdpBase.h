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
#ifndef LSST_QSERV_LOADER_SERVERBASE_H_
#define LSST_QSERV_LOADER_SERVERBASE_H_

// system headers
#include <cstdlib>
#include <iostream>
#include <boost/bind.hpp>
#include <boost/asio.hpp>

// Qserv headers
#include "loader/BufferUdp.h"

namespace lsst {
namespace qserv {
namespace loader {


using boost::asio::ip::udp;

class ServerUdpBase {
public:
    ServerUdpBase(boost::asio::io_service& io_service, short port);

    ServerUdpBase() = delete;
    ServerUdpBase(ServerUdpBase const&) = delete;
    ServerUdpBase& operator=(ServerUdpBase const&) = delete;

    virtual ~ServerUdpBase() = default;

    virtual BufferUdp::Ptr parseMsg(BufferUdp::Ptr const& data, udp::endpoint const& endpoint);

private:
    void _receiveCallBack(const boost::system::error_code& error, size_t bytes_recvd);
    void _sendCallBack(const boost::system::error_code& error, size_t bytes_sent);
    void _sendResponse();

    boost::asio::io_service& _ioService;
    udp::socket _socket;
    udp::endpoint _senderEndpoint;
    // char _data[MAX_MSG_SIZE];
    BufferUdp::Ptr _data{std::make_shared<BufferUdp>()}; ///< data buffer for recieving
    BufferUdp::Ptr _sendData; ///< data buffer for sending.

};



}}} // namespace lsst:qserv:loader

#endif
