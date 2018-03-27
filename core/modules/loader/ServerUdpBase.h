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

    uint64_t getNextMsgId() { return _msgIdSeq++; }
    std::string getOurHostName() { return _hostName; }
    short getOurPort() { return _port; }

private:
    void _receivePrepare(); ///< Give the io_service our callback for receiving.
    void _receiveCallback(const boost::system::error_code& error, size_t bytes_recvd);
    void _sendCallback(const boost::system::error_code& error, size_t bytes_sent);
    void _sendResponse(); ///< Send the contents of _sendData as a response;

    static std::atomic<uint64_t> _msgIdSeq; ///< Counter for unique message ids from this server.
    boost::asio::io_service& _ioService;
    udp::socket _socket;
    udp::endpoint _senderEndpoint;
    // char _data[MAX_MSG_SIZE];
    BufferUdp::Ptr _data; ///< data buffer for recieving
    BufferUdp::Ptr _sendData; ///< data buffer for sending.
    std::string _hostName;
    short _port;
};



}}} // namespace lsst:qserv:loader

#endif
