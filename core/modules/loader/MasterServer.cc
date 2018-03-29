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


// qserv headers
#include "loader/LoaderMsg.h"
#include "proto/ProtoImporter.h"
#include "proto/loader.pb.h"

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.MasterServer");
}

namespace lsst {
namespace qserv {
namespace loader {

BufferUdp::Ptr MasterServer::parseMsg(BufferUdp::Ptr const& data, udp::endpoint const& senderEndpoint) {

#if 0 // &&& old test code for echo may still be useful.
    // echo server, so send back what we got
    BufferUdp::Ptr sendData = data;
    std::string str(sendData->begin(), sendData->getCurrentLength());
    std::cout << "str len=" << str.length() << std::endl;
    LOGS(_log, LOG_LVL_INFO, "Master bytes(" << sendData->getCurrentLength() << "):(" << str <<
            ") from endpoint " << senderEndpoint);

#else
    LOGS(_log, LOG_LVL_INFO, "&&& MasterServer::parseMsg sender " << senderEndpoint << " data length=" << data->getCurrentWriteLength());
    BufferUdp::Ptr sendData; /// nullptr for empty response.
    LoaderMsg inMsg;
    inMsg.parseFromData(*data);
    LOGS(_log, LOG_LVL_INFO, "&&& MasterServer::parseMsg sender " << senderEndpoint <<
            " kind=" << inMsg.msgKind->element << " data length=" << data->getCurrentWriteLength());
    // &&& there are better ways to do this than a switch statement.
    switch (inMsg.msgKind->element) {
    case LoaderMsg::MSG_RECEIVED:
        // TODO: locate msg id in send messages and take action
        break;
    case LoaderMsg::MAST_INFO_REQ:
        // TODO: provide performance information about the master via MAST_INFO
        break;
    case LoaderMsg::MAST_WORKER_LIST_REQ:
        sendData = this->workerListRequest(inMsg, data, senderEndpoint);
        break;
    case LoaderMsg::MAST_WORKER_INFO_REQ:
        // TODO: Request information about a specific worker via MAST_WORKER_INFO
        break;
    case LoaderMsg::MAST_WORKER_ADD_REQ:
        sendData = workerAddRequest(inMsg, data, senderEndpoint);
        break;
    // following not expected by master
    case LoaderMsg::MAST_INFO:
    case LoaderMsg::MAST_WORKER_LIST:
    case LoaderMsg::MAST_WORKER_INFO:
    case LoaderMsg::WORKER_INSERT_KEY_REQ:
    case LoaderMsg::KEY_INFO_REQ:
    case LoaderMsg::KEY_INFO:
        /// &&& TODO add msg unexpected by master response.
        break;
    default:
        sendData = replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR, "unknownMsgKind");
    }
#endif


    return sendData;
}


BufferUdp::Ptr MasterServer::replyMsgReceived(udp::endpoint const& senderEndpoint,
                                                 LoaderMsg const& inMsg, int status, std::string const& msgTxt) {

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


BufferUdp::Ptr MasterServer::workerAddRequest(LoaderMsg const& inMsg, BufferUdp::Ptr const& data, udp::endpoint const& senderEndpoint) {

    proto::LdrMastWorkerAddReq addReq;

    //MsgElement::Ptr p = MsgElement::retrieve(*data); &&&
    StringElement::Ptr addReqData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (addReqData == nullptr) {
        return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR,
                                   "workerAddRequest protobuf string retrieve failed");
    }


    LOGS(_log, LOG_LVL_INFO, "MasterServer::workerAddRequest from " << senderEndpoint << " len=" << addReqData->element.length() <<
            " addReqData=(" << addReqData->element << ")");
    bool success = proto::ProtoImporter<proto::LdrMastWorkerAddReq>::setMsgFrom(addReq, addReqData->element.data(), addReqData->element.length());
    if (not success) {
        return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR, "parse error in workerAddRequest");
    }

    // TODO: This should be on separate thread
    _workerList->addWorker(addReq.workerip(), addReq.workerport());

    LOGS(_log, LOG_LVL_INFO, "Adding worker ip=" << addReq.workerip() << " port=" << addReq.workerport());

    return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_SUCCESS, "AddReq");
}


BufferUdp::Ptr MasterServer::workerListRequest(LoaderMsg const& inMsg, BufferUdp::Ptr const& data, udp::endpoint const& senderEndpoint) {
    proto::LdrMastWorkerAddReq addReq;

    //MsgElement::Ptr p = MsgElement::retrieve(*data); &&&
    StringElement::Ptr reqData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (reqData == nullptr) {
        return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR,
                                   "workerListRequest protobuf string retrieve failed");
    }


    LOGS(_log, LOG_LVL_INFO, "MasterServer::workerListRequest from " << senderEndpoint << " len=" << reqData->element.length() <<
            " reqData=(" << reqData->element << ")");
    bool success = proto::ProtoImporter<proto::LdrMastWorkerAddReq>::setMsgFrom(addReq, reqData->element.data(), reqData->element.length());
    if (not success) {
        return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR,
                                   "parse error in workerAddRequest");
    }
    LOGS(_log, LOG_LVL_INFO, "***&&& workerListRequest calling sendListTo " << senderEndpoint);
    // TODO: put this in a separate thread.
    _workerList->sendListTo(inMsg.msgId->element, addReq.workerip(), addReq.workerport(),
                            getOurHostName(), getOurPort());
    LOGS(_log, LOG_LVL_INFO, "***&&& workerListRequest done sendListTo ");

    return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_SUCCESS, "ListReq");
}


}}} // namespace lsst:qserv::loader


