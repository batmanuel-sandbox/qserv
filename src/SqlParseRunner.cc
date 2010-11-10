/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
 
// Boost
#include <boost/make_shared.hpp>

// Local (placed in src/)
#include "SqlSQL2Parser.hpp" 
#include "SqlSQL2Lexer.hpp"

#include "lsst/qserv/master/SqlParseRunner.h"
#include "lsst/qserv/master/Substitution.h"
#include "lsst/qserv/master/parseTreeUtil.h"
#include "lsst/qserv/master/stringUtil.h"

// namespace modifiers
namespace qMaster = lsst::qserv::master;
using std::stringstream;

// Anonymous helpers
namespace {
} // anonymous namespace

// Helper
class qMaster::LimitHandler : public VoidOneRefFunc {
public: 
    LimitHandler(qMaster::SqlParseRunner& spr) : _spr(spr) {}
    virtual ~LimitHandler() {}
    virtual void operator()(antlr::RefAST i) {
        std::stringstream ss(i->getText());
        int limit;
        ss >> limit;
        _spr._setLimitForHandler(limit);
        //std::cout << "Got limit -> " << limit << std::endl;            
    }
private:
    qMaster::SqlParseRunner& _spr;
};

class qMaster::OrderByHandler : public VoidOneRefFunc {
public: 
    OrderByHandler(qMaster::SqlParseRunner& spr) : _spr(spr) {}
    virtual ~OrderByHandler() {}
    virtual void operator()(antlr::RefAST i) {
        using qMaster::walkBoundedTreeString;
        using qMaster::getLastSibling;
        std::string cols = walkBoundedTreeString( i, getLastSibling(i));
        _spr._setOrderByForHandler(cols);
        //std::cout << "Got orderby -> " << cols << std::endl; 
    }
private:
    qMaster::SqlParseRunner& _spr;

};

// SpatialTableNotifier : receive notification that query has chosen a spatial
// table.  This can then trigger the preparation of the table metadata to 
// provide the context for the where-clause manipulator to rewrite 
// appropriately. 
class qMaster::SqlParseRunner::SpatialTableNotifier
    : public qMaster::Templater::Notifier {
public:
    SpatialTableNotifier(SqlParseRunner& spr) : _spr(spr) {}
    void operator()(std::string const& name) {
        _spr.prepareTableConfig(name);
        // FIXME: setup the right config.
        std::cout << "Picked " << name << " as spatial table." << std::endl;
    }
private:
    SqlParseRunner& _spr;
};

/// PartitionTupleProcessor : Function object that ingests config
/// entries from table.partitionCols.
/// e.g. table.partitionCols=Object:ra_PS,decl_PS,objectId;Source:raObject,declObject,objectId
/// First, split by ";", and then this object imports the resulting entries.
/// 
class qMaster::SqlParseRunner::PartitionTupleProcessor {
public:
    PartitionTupleProcessor(SqlParseRunner& spr) : _spr(spr) {}
    void operator()(std::string const& s) {
        vec.clear();
        tokenizeInto(s, ":", vec, passFunc<std::string>());
        if(vec.size() != 2) {
            if(vec.size() == 0) {
                return; // Nothing to do.
            }
            std::cout << "Error, badly formed partition col spec: " << s 
                      << std::endl;
            return;
        }
        std::string name = vec[0];
        columns.clear();
        tokenizeInto(vec[1], ",", columns, passFunc<std::string>());
        StringMap sm;
        sm["raCol"] = columns[0];
        sm["declCol"] = columns[1];
        sm["objectIdCol"] = columns[2];
        _spr.updateTableConfig(name, sm);
    }
    
    private:

    std::vector<std::string> vec;    
    std::vector<std::string> columns;
    SqlParseRunner& _spr;
};


boost::shared_ptr<qMaster::SqlParseRunner> 
qMaster::SqlParseRunner::newInstance(std::string const& statement, 
                                     std::string const& delimiter,
                                     qMaster::StringMap const& config) {
    return boost::shared_ptr<SqlParseRunner>(new SqlParseRunner(statement, 
                                                                delimiter,
                                                                config));
}

qMaster::SqlParseRunner::SqlParseRunner(std::string const& statement, 
                                        std::string const& delimiter,
                                        qMaster::StringMap const& config) :
    _statement(statement),
    _stream(statement, stringstream::in | stringstream::out),
    _factory(new ASTFactory()),
    _lexer(new SqlSQL2Lexer(_stream)),
    _parser(new SqlSQL2Parser(*_lexer)),
    _delimiter(delimiter),
    _spatialTableNotifier(new SpatialTableNotifier(*this)),
    _templater(delimiter, _factory.get(), *_spatialTableNotifier),
    _spatialUdfHandler(_factory.get(), _tableConfig),
    _aliasMgr(),
    _aggMgr(_aliasMgr)
{ 
    _readConfig(config);
    //std::cout << "(int)PARSING:"<< statement << std::endl;
}

void qMaster::SqlParseRunner::setup(std::list<std::string> const& names) {
    _templater.setKeynames(names.begin(), names.end());
    _parser->_columnRefHandler = _templater.newColumnHandler();
    _parser->_qualifiedNameHandler = _templater.newTableHandler();
    _tableListHandler = _templater.newTableListHandler();
    _parser->_tableListHandler = _tableListHandler;
    _parser->_setFctSpecHandler = _aggMgr.getSetFuncHandler();
    _parser->_columnAliasHandler = _aliasMgr.getColumnAliasHandler();
    _parser->_tableAliasHandler = _aliasMgr.getTableAliasHandler();
    _parser->_selectListHandler = _aggMgr.getSelectListHandler();
    _parser->_selectStarHandler = _aggMgr.newSelectStarHandler();
    _parser->_groupByHandler = _aggMgr.getGroupByHandler();
    _parser->_groupColumnHandler = _aggMgr.getGroupColumnHandler();
    _parser->_limitHandler.reset(new LimitHandler(*this));
    _parser->_orderByHandler.reset(new OrderByHandler(*this));
    _parser->_fromWhereHandler = _spatialUdfHandler.getFromWhereHandler();
    _parser->_whereCondHandler= _spatialUdfHandler.getWhereCondHandler();
    _parser->_qservRestrictorHandler = _spatialUdfHandler.getRestrictorHandler();
    _parser->_qservFctSpecHandler= _spatialUdfHandler.getFctSpecHandler();
}

std::string qMaster::SqlParseRunner::getParseResult() {
    if(_errorMsg.empty() && _parseResult.empty()) {
        _computeParseResult();
    }
    return _parseResult;
}
std::string qMaster::SqlParseRunner::getAggParseResult() {
    if(_errorMsg.empty() && _aggParseResult.empty()) {
        _computeParseResult();
    }
    return _aggParseResult;
}
void qMaster::SqlParseRunner::_computeParseResult() {
    bool hasBadDbs = false;
    try {
        _parser->initializeASTFactory(*_factory);
        _parser->setASTFactory(_factory.get());
        _parser->sql_stmt();
        _aggMgr.postprocess(_aliasMgr.getInvAliases());
        hasBadDbs = 0 < _templater.getBadDbs().size();
        RefAST ast = _parser->getAST();
        if (ast) {
            //std::cout << "fixupSelect " << getFixupSelect();
            //std::cout << "passSelect " << getPassSelect();
            // ";" is not in the AST, so add it back.
            _parseResult = walkTreeString(ast);
            _aggMgr.applyAggPass();
            _aggParseResult = walkTreeString(ast);
            if(_tableListHandler->getHasSubChunks()) {
                _makeOverlapMap();
                _aggParseResult = _composeOverlap(_aggParseResult);
                _parseResult = _composeOverlap(_parseResult);
            }
            _aggParseResult += ";";
            _parseResult += ";";
            _mFixup.select = _aggMgr.getFixupSelect();
            _mFixup.post = _aggMgr.getFixupPost();
            //"", /* FIXME need orderby */
            _mFixup.needsFixup = _aggMgr.getHasAggregate() 
                || (_mFixup.limit != -1) || (!_mFixup.orderBy.empty());
        } else {
            _errorMsg = "Error: no AST from parse";
        }
    } catch( antlr::ANTLRException& e ) {
        _errorMsg =  "Parse exception: " + e.toString();
    } catch( std::exception& e ) {
        _errorMsg = std::string("General exception: ") + e.what();
    }
    if(hasBadDbs) {
        std::stringstream ss;
        ss << " Query references prohibited dbs: ";
        Templater::StringList const& sl = _templater.getBadDbs();
        std::for_each(sl.begin(), sl.end(), coercePrint<std::string>(ss, ","));
        _errorMsg += ss.str();
    }
    return; 
}

void qMaster::SqlParseRunner::_makeOverlapMap() {
    qMaster::Templater::IntMap im = _tableListHandler->getUsageCount();
    qMaster::Templater::IntMap::iterator e = im.end();
    for(qMaster::Templater::IntMap::iterator i = im.begin(); i != e; ++i) {
        _overlapMap[i->first+"_sc2"] = i->first + "_sfo";
    }

}

std::string qMaster::SqlParseRunner::_composeOverlap(std::string const& query) {
    Substitution s(query, _delimiter, false);
    return query + " union " + s.transform(_overlapMap);
}

bool qMaster::SqlParseRunner::getHasAggregate() {
    if(_errorMsg.empty() && _parseResult.empty()) {
        _computeParseResult();
    }
    return _aggMgr.getHasAggregate();
}

void qMaster::SqlParseRunner::prepareTableConfig(std::string const& tableName) {
    // Hardcoded for Object table in PT1 schema right now.
    StringMap sm;
    _tableConfig = getFromMap(_tableConfigMap, tableName, sm);
    if(_tableConfig.size() == 0) {
        std::cout << "Error getting table config." << std::endl;
        _tableConfig["raCol"] = "ra_PS";
        _tableConfig["declCol"] = "decl_PS";
        _tableConfig["objectIdCol"] = "objectId";
    }
}

void qMaster::SqlParseRunner::updateTableConfig(std::string const& tName, 
                                                qMaster::StringMap const& m) {
    _tableConfigMap[tName] = m;
}

void qMaster::SqlParseRunner::_readConfig(qMaster::StringMap const& m) {
    std::string blank;
    std::list<std::string> tokens;
    std::string defaultDb;
    IntMap whiteList;
    // FIXME: Much of this could be done at startup and cached.
    defaultDb = getFromMap(m, "table.defaultdb", blank);

    tokenizeInto(getFromMap(m, "table.alloweddbs", blank), ",", tokens, 
                 passFunc<std::string>());
    if(tokens.size() > 0) {
        fillMapFromKeys(tokens, whiteList);
    } else {
        std::cout << "WARNING!  No dbs in whitelist. Using LSST." << std::endl;
        whiteList["LSST"] = 1;
    }    
    _templater.setup(whiteList, defaultDb);
    tokens.clear();
    tokenizeInto(getFromMap(m,"table.partitionCols", blank), ";", tokens,
                 passFunc<std::string>());
    for_each(tokens.begin(), tokens.end(), PartitionTupleProcessor(*this));
}    

    
