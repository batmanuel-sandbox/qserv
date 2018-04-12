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
#include "loader/Central.h"
#include "loader/LoaderMsg.h"
#include "loader/NetworkAddress.h"
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

BufferUdp::Ptr MasterServer::parseMsg(BufferUdp::Ptr const& data,
                                      boost::asio::ip::udp::endpoint const& senderEndpoint) {

    LOGS(_log, LOG_LVL_INFO, "&&& MasterServer::parseMsg sender " << senderEndpoint << " data length=" << data->getCurrentWriteLength());
    BufferUdp::Ptr sendData; /// nullptr for empty response.
    LoaderMsg inMsg;
    try {
        inMsg.parseFromData(*data);
    } catch (LoaderMsgErr const& exc) {
        std::string errMsg("MasterServer::parseMsg inMsg garbled exception ");
        errMsg += exc.what();
        LOGS(_log, LOG_LVL_ERROR, errMsg);
        sendData = replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR, errMsg);
        return sendData;
    }

    try {
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
            sendData = workerListRequest(inMsg, data, senderEndpoint);
            break;
        case LoaderMsg::MAST_WORKER_INFO_REQ:
            // TODO: Request information about a specific worker via MAST_WORKER_INFO
            sendData = workerInfoRequest(inMsg, data, senderEndpoint);
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
            ++_errCount;
            LOGS(_log, LOG_LVL_ERROR, "unknownMsgKind errCount=" << _errCount << " inMsg=" << inMsg);
            sendData = replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR, "unknownMsgKind");
        }
    } catch (LoaderMsgErr const& exc) {
        ++_errCount;
        std::string errMsg("MasterServer::parseMsg inMsg garbled exception ");
        errMsg += exc.what();
        LOGS(_log, LOG_LVL_ERROR, errMsg);
        // Send error back to the server in inMsg
        auto reply = replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR, errMsg);
        sendBufferTo(inMsg.senderHost->element, inMsg.senderPort->element, *reply);
        return nullptr;
    }

    return sendData;
}


BufferUdp::Ptr MasterServer::replyMsgReceived(boost::asio::ip::udp::endpoint const& senderEndpoint,
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


BufferUdp::Ptr MasterServer::workerAddRequest(LoaderMsg const& inMsg, BufferUdp::Ptr const& data,
                                              boost::asio::ip::udp::endpoint const& senderEndpoint) {

    /* &&&
    proto::LdrNetAddress addReq; // &&& rename addReq

    //MsgElement::Ptr p = MsgElement::retrieve(*data); &&&
    StringElement::Ptr addrData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (addrData == nullptr) {
        return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR,
                                   "workerAddRequest protobuf string retrieve failed");
    }


    LOGS(_log, LOG_LVL_INFO, "MasterServer::workerAddRequest from " << senderEndpoint << " len=" << addrData->element.length());
    bool success = proto::ProtoImporter<proto::LdrNetAddress>::setMsgFrom(addReq, addrData->element.data(), addrData->element.length());
    if (not success) {
        std::string errStr("STATUS_PARSE_ERR parse error in MasterServer::workerAddRequest");
        LOGS(_log, LOG_LVL_WARN, errStr);
        return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR, errStr);
    }


    // TODO: This should be on separate thread &&&
    _centralMaster->addWorker(addReq.workerip(), addReq.workerport());
    // _workerList->addWorker(addReq.workerip(), addReq.workerport()); &&&


    LOGS(_log, LOG_LVL_INFO, "Adding worker ip=" << addReq.workerip() << " port=" << addReq.workerport());

    // return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_SUCCESS, "AddReq"); &&&
    return nullptr;
    */

    /// Message contains the network address of a worker to add to our list.
    auto addReq = NetworkAddress::create(data, "MasterServer::workerAddRequest");
    if (addReq == nullptr) {
        return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR,
                "STATUS_PARSE_ERR parse error workerAddRequest ");
    }

    // Once the worker has been added, its name will be sent to all other workers.
    _centralMaster->addWorker(addReq->ip, addReq->port);

    LOGS(_log, LOG_LVL_INFO, "Adding worker ip=" << addReq->ip << " port=" << addReq->port);

    return nullptr;
}


BufferUdp::Ptr MasterServer::workerListRequest(LoaderMsg const& inMsg, BufferUdp::Ptr const& data,
                                               boost::asio::ip::udp::endpoint const& senderEndpoint) {
    /* &&&
    LOGS(_log, LOG_LVL_INFO, "  &&& MasterServer::workerListRequest");

    proto::LdrNetAddress addReq;

    //MsgElement::Ptr p = MsgElement::retrieve(*data); &&&
    StringElement::Ptr reqData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
    if (reqData == nullptr) {
        std::string errStr("STATUS_PARSE_ERR parse error in MasterServer::workerListRequest");
        LOGS(_log, LOG_LVL_WARN, errStr);
        return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR, errStr);
    }

    LOGS(_log, LOG_LVL_INFO, "MasterServer::workerListRequest from " << senderEndpoint << " len=" << reqData->element.length());
    bool success = proto::ProtoImporter<proto::LdrNetAddress>::setMsgFrom(addReq, reqData->element.data(), reqData->element.length());
    if (not success) {
        return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR,
                                   "parse error in workerAddRequest");
    }
    */

    std::string funcName("MasterServer::workerListRequest");
    LOGS(_log, LOG_LVL_INFO, "  &&& " << funcName);

    auto addr = NetworkAddress::create(data, funcName);
    if (addr == nullptr) {
        std::string errStr("STATUS_PARSE_ERR parse error in " + funcName);
        LOGS(_log, LOG_LVL_ERROR, errStr);
        return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR, errStr);
    }

    /* &&&
    LOGS(_log, LOG_LVL_INFO, "***&&& workerListRequest calling sendListTo " << senderEndpoint);
    // TODO: put this in a separate thread.
    auto workerList = _centralMaster->getWorkerList();
    workerList->sendListTo(inMsg.msgId->element, addReq.workerip(), addReq.workerport(),
                           getOurHostName(), getOurPort());
    LOGS(_log, LOG_LVL_INFO, "***&&& workerListRequest done sendListTo ");

    // return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_SUCCESS, "ListReq"); &&&
    return nullptr;
    */

    LOGS(_log, LOG_LVL_INFO, "&&& workerListRequest calling sendListTo " << senderEndpoint);
    // TODO: put this in a separate thread.
    auto workerList = _centralMaster->getWorkerList();
    workerList->sendListTo(inMsg.msgId->element, addr->ip, addr->port, getOurHostName(), getOurPort());
    LOGS(_log, LOG_LVL_INFO, "&&& workerListRequest done sendListTo ");

    return nullptr;
}


BufferUdp::Ptr MasterServer::workerInfoRequest(LoaderMsg const& inMsg, BufferUdp::Ptr const& data,
                                 boost::asio::ip::udp::endpoint const& senderEndpoint) {
    LOGS(_log, LOG_LVL_INFO, "  &&& MasterServer::workerInfoRequest **************");

    try {
        std::string const funcName("MasterServer::workerInfoRequest");
        NetworkAddress::UPtr nAddr = NetworkAddress::create(data, funcName);
        if (nAddr == nullptr) {
            throw LoaderMsgErr(funcName, __FILE__, __LINE__);
        }

        /* &&&
        proto::WorkerListItem protoItem;
        StringElement::Ptr itemData = std::dynamic_pointer_cast<StringElement>(MsgElement::retrieve(*data));
        if (itemData == nullptr) {
            throw LoaderMsgErr(funcName, __FILE__, __LINE__);
        }
        LOGS(_log, LOG_LVL_INFO, funcName << " from " << senderEndpoint << " len=" << itemData->element.length());
        auto wName = itemData->protoParse<proto::WorkerListItem>();
        */
        auto protoItem = StringElement::protoParse<proto::WorkerListItem>(data);
        if (protoItem == nullptr) {
            throw LoaderMsgErr(funcName, __FILE__, __LINE__);
        }

        auto workerName = protoItem->name();
        LOGS(_log, LOG_LVL_INFO, "************************* &&& Master got name=" << workerName);

        /// &&& find the worker name in the map.

        /// &&& return worker's name, netaddress, and range



    } catch (LoaderMsgErr &msgErr) {
        LOGS(_log, LOG_LVL_ERROR, msgErr.what());
        return replyMsgReceived(senderEndpoint, inMsg, LoaderMsg::STATUS_PARSE_ERR, msgErr.what());
    }
    return nullptr;
}

}}} // namespace lsst:qserv::loader


