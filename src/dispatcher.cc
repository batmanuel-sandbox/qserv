// -*- LSST-C++ -*-

#include "lsst/qserv/master/xrdfile.h"
#include "lsst/qserv/master/dispatcher.h"
#include "lsst/qserv/master/thread.h"
#include "lsst/qserv/master/xrootd.h"
#include "lsst/qserv/master/SessionManager.h"
#include "lsst/qserv/master/AsyncQueryManager.h"

namespace qMaster = lsst::qserv::master;

using lsst::qserv::master::SessionManager;
typedef SessionManager<qMaster::AsyncQueryManager::Ptr> SessionMgr;
typedef boost::shared_ptr<SessionMgr> SessionMgrPtr;
namespace {

    SessionMgr& getSessionManager() {
	// Singleton for now.
	static SessionMgrPtr sm;
	if(sm.get() == NULL) {
	    sm = boost::make_shared<SessionMgr>();
	}
	assert(sm.get() != NULL);
	return *sm;
    }

    // Deprecated
    qMaster::QueryManager& getManager(int session) {
	// Singleton for now. //
	static boost::shared_ptr<qMaster::QueryManager> qm;
	if(qm.get() == NULL) {
	    qm = boost::make_shared<qMaster::QueryManager>();
	}
	return *qm;
    }

    qMaster::AsyncQueryManager& getAsyncManager(int session) {
	return *(getSessionManager().getSession(session));
    }

    
}

void qMaster::initDispatcher() {
    xrdInit();
}

/// @param session Int for the session (the top-level query)
/// @param chunk Chunk number within this session (query)
/// @param str Query string (c-string)
/// @param len Query string length
/// @param savePath File path (with name) which will store the result (file, not dir)
/// @return a token identifying the session
int qMaster::submitQuery(int session, int chunk, char* str, int len, char* savePath, std::string const& resultName) {
    TransactionSpec t;
    t.chunkId = chunk;
    t.query = std::string(str, len);
    t.bufferSize = 8192000;
    t.path = qMaster::makeUrl("query", chunk);
    t.savePath = savePath;
    return submitQuery(session, TransactionSpec(t), resultName);
}

int qMaster::submitQuery(int session, qMaster::TransactionSpec const& s, std::string const& resultName) {
    int queryId = 0;
#if 1
    AsyncQueryManager& qm = getAsyncManager(session);
    qm.add(s, resultName); 
    //std::cout << "Dispatcher added  " << s.chunkId << std::endl;

#else
    QueryManager& qm = getManager(session);
    qm.add(s); 
#endif
    /* queryId = */ 
    return queryId;
}

qMaster::QueryState qMaster::joinQuery(int session, int id) {
    // Block until specific query id completes.
#if 1
    AsyncQueryManager& qm = getAsyncManager(session);
#else
    QueryManager& qm = getManager(session);
#endif
    //qm.join(id);
    //qm.status(id); // get status
    // If error, report
    return UNKNOWN; // FIXME: convert status to querystate.
}

qMaster::QueryState qMaster::tryJoinQuery(int session, int id) {
#if 1
    AsyncQueryManager& qm = getAsyncManager(session);
#else
    QueryManager& qm = getManager(session);
#endif
#if 0 // Not implemented yet
    // Just get the status and return it.
    if(qm.tryJoin(id)) {
	return SUCCESS; 
    } else {
	return ERROR;
    }   
#endif
}

struct mergeStatus {
    mergeStatus(bool& success) : isSuccessful(success) {isSuccessful = true;}
    void operator() (qMaster::AsyncQueryManager::Result const& x) { 
	if(! x.second.isSuccessful()) {
	    std::cout << "Chunk " << x.first << " error " << std::endl
		      << "open: " << x.second.open 
		      << " qWrite: " << x.second.queryWrite 
		      << " read: " << x.second.read 
		      << " lWrite: " << x.second.localWrite << std::endl;
	    isSuccessful = false;
	} else {
	    std::cout << "Chunk " << x.first << " successful with "
		      << x.second.localWrite << std::endl;
	}
    }
    bool& isSuccessful;
};

qMaster::QueryState qMaster::joinSession(int session) {
    AsyncQueryManager& qm = getAsyncManager(session);
    qm.joinEverything();
    AsyncQueryManager::ResultDeque const& d = qm.getFinalState();
    bool successful;
    std::for_each(d.begin(), d.end(), mergeStatus(successful));
    
    std::cout << "Joined everything" << std::endl;
    if(successful) {
	std::cout << "Successful!" << std::endl;
	return SUCCESS;
    } else {
	std::cout << "Failure!" << std::endl;
	return ERROR;
    }
}

std::string const& qMaster::getQueryStateString(QueryState const& qs) {
    static const std::string unknown("unknown");
    static const std::string waiting("waiting");
    static const std::string dispatched("dispatched");
    static const std::string success("success");
    static const std::string error("error");
    switch(qs) {
    case UNKNOWN:
	return unknown;
    case WAITING:
	return waiting;
    case DISPATCHED:
	return dispatched;
    case SUCCESS:
	return success;
    case ERROR:
	return error;
    default:
	return unknown;
    }
    

}


int qMaster::newSession() {
    AsyncQueryManager::Ptr m = boost::make_shared<qMaster::AsyncQueryManager>();
    int id = getSessionManager().newSession(m);
    return id;
}

void qMaster::configureSessionMerger(int session, TableMergerConfig const& c) {
    getAsyncManager(session).configureMerger(c);    
}

std::string qMaster::getSessionResultName(int session) {
    return getAsyncManager(session).getMergeResultName();
}

void qMaster::discardSession(int session) {
    getSessionManager().discardSession(session);
    return; 
}

qMaster::XrdTransResult qMaster::getQueryResult(int session, int chunk) {
    // FIXME
}
