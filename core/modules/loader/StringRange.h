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
#ifndef LSST_QSERV_LOADER_STRINGRANGE_H_
#define LSST_QSERV_LOADER_STRINGRANGE_H_

// system headers
#include <memory>
#include <string>


// Qserv headers



namespace lsst {
namespace qserv {
namespace loader {


class StringRange {
public:
    using Ptr = std::shared_ptr<StringRange>;

    StringRange() = default;
    StringRange(StringRange const&) = default;
    StringRange& operator=(StringRange const&) = default;

    ~StringRange() = default;

    void setAllInclusiveRange() {
        _min = "";
        _largest = true;
        setValid();
    }

    bool setMin(std::string const& val) {
        if (not _largest && val > _max) {
            return false;
        }
        _min = val;
    }

    bool setMax(std::string const& val, bool largest=false) {
        if (largest) {
            _largest = true;
            if (val > _max) { _max = val; }
            return true;
        }
        if (val < _min) {
            return false;
        }
        _max = val;
        return true;
    }

    bool setMinMax(std::string const& vMin, std::string const& vMax, bool largest=false) {
        if (largest) {
            _largest = true;
            _min = vMin;
            _max = (vMax > _min) ? vMax : _min;
            return true;
        }
        if (vMin > vMax) { return false; }
        _min = vMin;
        _max = vMax;
        setValid();
        return true;
    }

    bool setValid() {
        if (!_largest && _max < _min) {
            return false;
        }
        _valid = true;
        return true;
    }

    bool getValid() const { return _valid; }
    bool getLargest() const { return _largest; }
    std::string getMin() const { return _min; }
    std::string getMax() const { return _max; }



private:
    bool        _valid{false}; ///< true if range is valid
    bool        _largest{false}; ///< true if the range includes largest possible values.
    std::string _min; ///< Smallest value = ""
    std::string _max; ///<

};


}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_STRINGRANGE_H_

