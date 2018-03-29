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
#include "loader/WorkerServer.h"

// System headers
#include <iostream>

// Third-party headers

// Qserv headers
#include "loader/LoaderMsg.h"
#include "proto/loader.pb.h"

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.WorkerServer");
}

namespace lsst {
namespace qserv {
namespace loader {

BufferUdp::Ptr WorkerServer::parseMsg(BufferUdp::Ptr const& data, udp::endpoint const& senderEndpoint) {
#if 0 // &&& old test code for echo may still be useful.
    // echo server, so send back what we got
    BufferUdp::Ptr sendData = data;
    std::string str(sendData->begin(), sendData->getCurrentLength());
    std::cout << "str len=" << str.length() << std::endl;
    LOGS(_log, LOG_LVL_INFO, "Worker bytes(" << sendData->getCurrentLength() << "):(" << str <<
            ") from endpoint " << senderEndpoint);
#else
    LOGS(_log, LOG_LVL_INFO, "&&& WorkerServer::parseMsg sender " << senderEndpoint << " data length=" << data->getCurrentWriteLength());
    BufferUdp::Ptr sendData; /// nullptr for empty response.
    LoaderMsg inMsg;
    inMsg.parseFromData(*data);
    LOGS(_log, LOG_LVL_INFO, "&&& WorkerServer::parseMsg sender " << senderEndpoint <<
            " kind=" << inMsg.msgKind->element << " data length=" << data->getCurrentWriteLength());
    // &&& there are better ways to do this than a switch statement.
    switch (inMsg.msgKind->element) {
    case LoaderMsg::MAST_INFO:
        // TODO handle a message with information about the master
        break;
    case LoaderMsg::MAST_WORKER_LIST:
        _workersList->workerListReceive(data);
        break;
    case LoaderMsg::MAST_WORKER_INFO:
    case LoaderMsg::WORKER_INSERT_KEY_REQ:
    case LoaderMsg::KEY_INFO_REQ:
    case LoaderMsg::KEY_INFO:


        // following not expected by worker
    case LoaderMsg::MSG_RECEIVED:
    case LoaderMsg::MAST_INFO_REQ:
    case LoaderMsg::MAST_WORKER_LIST_REQ:
    case LoaderMsg::MAST_WORKER_INFO_REQ:
    case LoaderMsg::MAST_WORKER_ADD_REQ:
        // TODO add response for known but unexpected message.
    default:
        sendData = replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR, "unknownMsgKind");
    }


#endif

    return sendData;
}


BufferUdp::Ptr WorkerServer::replyMsgReceived(udp::endpoint const& senderEndpoint, LoaderMsg const& inMsg,
                                              int status, std::string const& msgTxt) {

    if (status != LoaderMsg::STATUS_SUCCESS) {
        LOGS(_log,LOG_LVL_WARN, "Error response Original from " << senderEndpoint <<
                " msg=" << msgTxt << " inMsg=" << inMsg.getStringVal());
    }

    LoaderMsg outMsg(LoaderMsg::MSG_RECEIVED, inMsg.msgId->element, getOurHostName(), getOurPort());

    // create the proto buffer
    proto::LdrMsgReceived protoBuf;
    protoBuf.set_originalid(inMsg.msgId->element);
    protoBuf.set_originalkind(inMsg.msgKind->element);
    protoBuf.set_status(LoaderMsg::STATUS_PARSE_ERR);
    protoBuf.set_errmsg(msgTxt);
    protoBuf.set_dataentries(0);

    StringElement respBuf;
    protoBuf.SerializeToString(&(respBuf.element));

    auto sendData = std::make_shared<BufferUdp>(1000); // this message should be fairly small.
    outMsg.serializeToData(*sendData);
    respBuf.appendToData(*sendData);
    return sendData;
}



}}} // namespace lsst:qserv::loader





