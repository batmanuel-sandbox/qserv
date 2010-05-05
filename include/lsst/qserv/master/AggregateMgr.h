#ifndef LSST_QSERV_MASTER_AGGREGATEMGR_H
#define LSST_QSERV_MASTER_AGGREGATEMGR_H

// C++ 
#include <deque>
#include <map>

// Boost
#include <boost/shared_ptr.hpp>

// ANTLR
//#include "antlr/AST.hpp"
//#include "antlr/CommonAST.hpp"
#include "antlr/ASTRefCount.hpp"

// Package:
#include "lsst/qserv/master/parserBase.h" 
#include "lsst/qserv/master/parseTreeUtil.h"

namespace lsst {
namespace qserv {
namespace master {

typedef std::pair<antlr::RefAST, antlr::RefAST> NodeBound;
typedef std::deque<NodeBound> NodeList;
typedef NodeList::const_iterator NodeListConstIter;

class AggregateRecord {
public:
    NodeBound lbl;
    NodeBound meaning;
    std::string orig; // Original
    std::string pass; // Subquery
    std::string fixup; // Merging/fixup
    std::ostream& printTo(std::ostream& os) {
	os << "Aggregate orig=" << orig << std::endl 
	   << "pass=" << pass  << std::endl
	   << "fixup=" << fixup;
    }
};

class AggregateMgr {
public:

    class AliasVal {
    public:
	AliasVal(antlr::RefAST lbl_, antlr::RefAST meaning_) : lbl(lbl_), meaning(meaning_){}
	antlr::RefAST lbl;
	antlr::RefAST meaning;
    };
    typedef std::map<antlr::RefAST, AggregateRecord> AggMap;
    class AggBuilderIf {
    public:
	typedef boost::shared_ptr<AggBuilderIf> Ptr;
	virtual AggregateRecord operator()(NodeBound const& lbl,
					   NodeBound const& meaning) = 0;
    };
    class EasyAggBuilder : public AggBuilderIf {
    public:
	virtual AggregateRecord operator()(NodeBound const& lbl,
					   NodeBound const& meaning);
	std::string computeFixup(NodeBound meaning, NodeBound lbl); 
    };
    class CountAggBuilder {
    };
    class AvgAggBuilder {
    };
    
    class SetFuncHandler : public VoidOneRefFunc {
    public: 
	typedef std::map<std::string, AggBuilderIf::Ptr> Map;
	typedef Map::const_iterator MapConstIter;
	typedef Map::iterator MapIter;

	typedef std::deque<NodeBound> Deque;
	typedef Deque::const_iterator DequeConstIter;
	typedef Deque::iterator DequeIterator;
	
	SetFuncHandler();
	virtual ~SetFuncHandler() {}
	virtual void operator()(antlr::RefAST a);
	Deque const& getAggs() const { return _aggs; }
	Map& getProcs() { return _map; }
    private:
	Deque _aggs;
	Map _map;
    }; // class SetFuncHandler
    class AliasHandler : public VoidTwoRefFunc {
    public: 
	typedef std::map<antlr::RefAST, NodeBound> Map;
	typedef Map::const_iterator MapConstIter;
	typedef Map::iterator MapIter;

	AliasHandler() {}
	virtual ~AliasHandler() {}
	virtual void operator()(antlr::RefAST a, antlr::RefAST b)  {
	    using lsst::qserv::master::getLastSibling;
	    if(b.get()) {
		_map[a] = NodeBound(b, getLastSibling(a));
	    }
	    _nodes.push_back(NodeBound(a, getLastSibling(a)));
	    // Save column ref for pass/fixup computation, 
	    // regardless of alias.
	}
	Map const& getInvAliases() const { return _map; }
	NodeList getNodeListCopy() { return _nodes; }
	void resetNodeList() { _nodes.clear(); }
    private:
	Map _map;
	NodeList _nodes;
    }; // class AliasHandler

    class SelectListHandler : public VoidOneRefFunc {
    public: 
	class SelectStarHandler : public VoidVoidFunc {
	public: 
	    SelectStarHandler(SelectListHandler& h) : handler(h) {}
	    virtual ~SelectStarHandler() {}
	    virtual void operator()() { handler.handleSelectStar(); }
	    SelectListHandler& handler;
	};
	// typedef std::deque<antlr::RefAST> SelectList;
	// typedef SelectList::const_iterator SelectListConstIter;
	// typedef std::deque<SelectList> Deque;
	typedef std::deque<NodeList> Deque;
	SelectListHandler(AliasHandler& h);
	virtual ~SelectListHandler() {}
	virtual void operator()(antlr::RefAST a);
	void handleSelectStar() { 
	    if(selectLists.empty()) { 
		isStarFirst = true; 
	    } 
	}
	boost::shared_ptr<SelectStarHandler> getSelectStarHandler() {
	    typedef boost::shared_ptr<SelectStarHandler> Ptr;
	    return Ptr(new SelectStarHandler(*this));
	}
	AliasHandler& _aHandler; // Get help from AliasHandler
	Deque selectLists;
	NodeBound firstSelectBound;
	bool isStarFirst;
    }; // class SelectListHandler
    
    AggregateMgr();
    
    void postprocess();
    void applyAggPass();
    std::string getPassSelect();
    std::string getFixupSelect();
    void _computeSelects();

    boost::shared_ptr<VoidTwoRefFunc> getAliasHandler() {return _aliaser;}
    boost::shared_ptr<VoidOneRefFunc> getSetFuncHandler() {return _setFuncer;}
    boost::shared_ptr<VoidOneRefFunc> getSelectListHandler() {return _selectLister;}
    boost::shared_ptr<VoidVoidFunc> getSelectStarHandler() {
	return _selectLister->getSelectStarHandler();
    }
    std::string _passSelect;
    std::string _fixupSelect;
    
private:
    boost::shared_ptr<AliasHandler> _aliaser;
    boost::shared_ptr<SetFuncHandler> _setFuncer;
    boost::shared_ptr<SelectListHandler> _selectLister;
    AggMap _aggRecords;
}; // class AggregateMgr

}}} // lsst::qserv::master
#endif // LSST_QSERV_MASTER_AGGREGATEMGR_H
