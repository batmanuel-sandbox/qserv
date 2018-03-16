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
#include "loader/ServerUdpBase.h"

// System headers
#include <iostream>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.ServerUdpBase");
}

namespace lsst {
namespace qserv {
namespace loader {


ServerUdpBase::ServerUdpBase(boost::asio::io_service& io_service, short port) : _ioService(io_service),
_socket(io_service, udp::endpoint(udp::v4(), port)) {
    _socket.async_receive_from(
        boost::asio::buffer(_data->getBuffer(), _data->getMaxLength()), _senderEndpoint,
        boost::bind(&ServerUdpBase::_receiveCallBack, this,
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred));
}


void ServerUdpBase::_receiveCallBack(boost::system::error_code const& error, size_t bytes_recvd) {
    _data->setWriteCursor(bytes_recvd); // _data needs to know the valid portion of the buffer.
    if (!error && bytes_recvd > 0) {
        std::string str(_data->begin(), bytes_recvd);
        std::cout << "str len=" << str.length() << std::endl;
        LOGS(_log, LOG_LVL_INFO, "rCB received(" << bytes_recvd << "):" << str <<
                                 ", error code: " << error << ", from endpoint " << _senderEndpoint);

        _sendData = parseMsg(_data, _senderEndpoint);
        _sendResponse();
    } else {
        /// TODO - echoing is not good error response behavior.
        _sendData = _data;
        _sendResponse();
    }
    _data = std::make_shared<BufferUdp>(); // new buffer for next call
}


void ServerUdpBase::_sendResponse() {
    _socket.async_send_to(boost::asio::buffer(_sendData->begin(), _sendData->getCurrentWriteLength()),
                                              _senderEndpoint,
                          boost::bind(&ServerUdpBase::_sendCallBack, this,
                                      boost::asio::placeholders::error,
                                      boost::asio::placeholders::bytes_transferred));
}


/// This function, and its derived children, should return quickly. Handing 'data' off to another thread
/// for handling is safe.
BufferUdp::Ptr ServerUdpBase::parseMsg(BufferUdp::Ptr const& data, udp::endpoint const& senderEndpoint) {
    // echo server, so send back what we got
    BufferUdp::Ptr sendData = data;
    std::string str(sendData->begin(), sendData->getCurrentLength());
    std::cout << "str len=" << str.length() << std::endl;
    LOGS(_log, LOG_LVL_INFO, "pM bytes(" << sendData->getCurrentLength() << "):(" << str <<
                             ") from endpoint " << senderEndpoint);

    return sendData;
}


void ServerUdpBase::_sendCallBack(const boost::system::error_code& error, size_t bytes_sent) {
    LOGS(_log, LOG_LVL_INFO, " _sendCallBack bytes_sent=" << bytes_sent);
    _socket.async_receive_from(boost::asio::buffer(_data->getBuffer(), _data->getMaxLength()), _senderEndpoint,
                               boost::bind(&ServerUdpBase::_receiveCallBack, this,
                                           boost::asio::placeholders::error,
                                           boost::asio::placeholders::bytes_transferred));
}


}}} // namespace lsst::qserrv::loader
