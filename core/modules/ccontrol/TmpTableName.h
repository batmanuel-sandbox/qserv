// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2017 LSST Corporation.
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

#ifndef LSST_QSERV_CCONTROL_TMPTABLENAME_H
#define LSST_QSERV_CCONTROL_TMPTABLENAME_H

// System headers
#include <string>
#include <sstream>

// Qserv headers
#include "global/intTypes.h"
#include "util/StringHash.h"

namespace lsst {
namespace qserv {
namespace ccontrol {

/// TmpTableName : a generator for temporary table names for chunk results.
/// All member variables must be immutable.
class TmpTableName {
public:
    TmpTableName(QueryId qId, std::string const& query) : _prefix(_makePrefix(qId, query)) {}

    std::string make(int chunkId, int seq=0) const {
        std::stringstream ss;
        ss << _prefix << chunkId << "_" << seq;
        return ss.str();
    }
private:
    std::string _makePrefix(QueryId qId, std::string const& query) const {
        std::stringstream ss;
        ss << "r_" << qId << "_" << util::StringHash::getMd5Hex(query.data(), query.size()) << "_";
        return ss.str();
    }
    std::string const _prefix;
};

}}} // namespace lsst::qserv:ccontrol

#endif // LSST_QSERV_CCONTROL_TMPTABLENAME_H
