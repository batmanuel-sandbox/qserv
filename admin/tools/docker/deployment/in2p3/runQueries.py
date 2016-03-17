#!/usr/bin/env python

# LSST Data Management System
# Copyright 2015 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

"""
A test program that runs queries in parallel. The queries will run
with the database we have on the IN2P3 cluster for the Summer 2015
test.

@author  Jacek Becla, SLAC
"""

import commands
import logging
import os
import pprint
import random
import threading
import time

import MySQLdb

###############################################################################
# Queries to run, grouped into different pools of queries
###############################################################################

queryPools = {}

OUTPUT_DIR="/sps/lsst/Qserv/fjammes/runQueries"

# Low Volume Queries
queryPools["LV"] = []
for i in range(0, 10):
    # single object
    queryPools["LV"].append("SELECT ra, decl, raVar, declVar, radeclCov, u_psfFlux, u_psfFluxSigma, u_apFlux FROM Object WHERE deepSourceId = %d" % random.randint(2251799813685248, 4503595332407303))

    # small area selection
    raMin = random.uniform(0, 350)
    declMin = random.uniform(-87, 45)
    raDist = random.uniform(0.01, 0.2)
    declDist = random.uniform(0.01, 0.2)
    queryPools["LV"].append("SELECT ra, decl, raVar, declVar, radeclCov, u_psfFlux, u_psfFluxSigma, u_apFlux FROM Object WHERE qserv_areaspec_box(%f, %f, %f, %f)" % (raMin, declMin, raMin+raDist, declMin+declDist))

    # small area join
    raMin = random.uniform(0, 350)
    declMin = random.uniform(-87, 45)
    raDist = random.uniform(0.01, 0.1)
    declDist = random.uniform(0.01, 0.1)
    queryPools["LV"].append("SELECT o.deepSourceId, o.ra, o.decl, s.coord_ra, s.coord_decl, s.parent FROM Object o, Source s WHERE qserv_areaspec_box(%f, %f, %f, %f) and o.deepSourceId = s.objectId" % (raMin, declMin, raMin+raDist, declMin+declDist))


# Full-table-scans on Object
queryPools["FTSObj"] = [
    "SELECT COUNT(*) FROM Object WHERE y_instFlux > 0.05",
    "SELECT ra, decl, u_psfFlux, g_psfFlux, r_psfFlux FROM Object WHERE y_shapeIxx BETWEEN 20 AND 40",
    "SELECT COUNT(*) FROM Object WHERE y_instFlux > u_instFlux",
    "SELECT MIN(ra), MAX(ra) FROM Object WHERE decl > 3",
    "SELECT MIN(ra), MAX(ra) FROM Object WHERE z_apFlux BETWEEN 1 and 2",
    "SELECT MIN(ra), MAX(ra), MIN(decl), MAX(decl) FROM Object",
    "SELECT MIN(ra), MAX(ra), MIN(decl), MAX(decl) FROM Object WHERE z_instFlux < 3",
    "SELECT COUNT(*) AS n, AVG(ra), AVG(decl), chunkId FROM Object GROUP BY chunkId",
###  "SELECT deepSourceId, u_apFluxSigma FROM Object WHERE u_apFluxSigma between 0 and 0.02",     # 1,889,695,615 rows / ~28 GB
###  "SELECT deepSourceId, u_apFluxSigma FROM Object WHERE u_apFluxSigma between 0 and 2.27e-30", #   475,244,843 rows / ~ 7 GB
#    "SELECT deepSourceId, u_apFluxSigma FROM Object WHERE u_apFluxSigma between 0 and 2e-30",     #    42,021,567 rows / ~ 0.5 GB
    "SELECT deepSourceId, u_apFluxSigma FROM Object WHERE u_apFluxSigma between 0 and 1.75e-30",   #     1,932,988 rows / ~ 29 MB
    "SELECT deepSourceId, u_apFluxSigma FROM Object WHERE u_apFluxSigma between 0 and 1.8e-30",
    "SELECT deepSourceId, u_apFluxSigma FROM Object WHERE u_apFluxSigma between 0 and 1.81e-30",
    "SELECT deepSourceId, u_apFluxSigma FROM Object WHERE u_apFluxSigma between 0 and 1.5e-30"     #       119,423 rows / ~ 2 MB
]

# Full-table-scans on Source
queryPools["FTSSrc"] = [
    "SELECT COUNT(*) FROM Source WHERE flux_sinc BETWEEN 1 AND 2"
]

# Full-table-scans on ForcedSource
queryPools["FTSFSrc"] = [
    "SELECT COUNT(*) FROM ForcedSource WHERE psfFlux BETWEEN 0.1 AND 0.2"
]

# Object-Source Joins
queryPools["joinObjSrc"] = [
    "SELECT o.deepSourceId, s.objectId, s.id, o.ra, o.decl FROM Object o, Source s WHERE o.deepSourceId=s.objectId AND s.flux_sinc BETWEEN 0.13 AND 0.14",
    "SELECT o.deepSourceId, s.objectId, s.id, o.ra, o.decl FROM Object o, Source s WHERE o.deepSourceId=s.objectId AND s.flux_sinc BETWEEN 0.3 AND 0.31",
    "SELECT o.deepSourceId, s.objectId, s.id, o.ra, o.decl FROM Object o, Source s WHERE o.deepSourceId=s.objectId AND s.flux_sinc BETWEEN 0.7 AND 0.72"
]

# Object-ForcedSource Joins
queryPools["joinObjFSrc"] = [
    "SELECT o.deepSourceId, f.psfFlux FROM Object o, ForcedSource f WHERE o.deepSourceId=f.deepSourceId AND f.psfFlux BETWEEN 0.13 AND 0.14"
]

# Near neighbor
queryPools["nearN"] = []
for i in range(0,10):
    raMin = random.uniform(0, 340)
    declMin = random.uniform(-87, 40)
    raDist = random.uniform(8, 12)
    declDist = random.uniform(8, 12)
#    queryPools["nearN"].append("select o1.ra as ra1, o2.ra as ra2, o1.decl as decl1, o2.decl as decl2, scisql_angSep(o1.ra, o1.decl,o2.ra, o2.decl) AS theDistance from Object o1, Object o2 where qserv_areaspec_box(%f, %f, %f, %f) and scisql_angSep(o1.ra, o1.decl, o2.ra, o2.decl) < 0.015" % (raMin, declMin, raMin+raDist, declMin+declDist))
    queryPools["nearN"].append("select count(*) from Object o1, Object o2 where qserv_areaspec_box(%f, %f, %f, %f) and scisql_angSep(o1.ra, o1.decl, o2.ra, o2.decl) < 0.015" % (raMin, declMin, raMin+raDist, declMin+declDist))

pp = pprint.PrettyPrinter(indent=4)
pp.pprint(queryPools)

###############################################################################
# Definition of how many queries from each pool we want to run simultaneously
###############################################################################

concurrency = {
    "LV": 75,
    "FTSObj": 3,
    "FTSSrc": 1,
    "FTSFSrc": 1,
    "joinObjSrc": 1,
    "joinObjFSrc": 1,
    "nearN": 1
}

# how long a query should take in seconds
targetRates = {
    "LV": 10,
    "FTSObj": 3600,
    "FTSSrc": 3600*12,
    "FTSFSrc": 3600*12,
    "joinObjSrc": 3600*12,
    "joinObjFSrc": 3600*12,
    "nearN": 3600
}

# time that we exceeded, if that happens, we won't sleep after future queries
# that finished earlier than planned to recover that lost time

timeBehind = {
    "LV": 0,
    "FTSObj": 0,
    "FTSSrc": 0,
    "FTSFSrc": 0,
    "joinObjSrc": 0,
    "joinObjFSrc": 0,
    "nearN": 0
}

timeBehindMutex = threading.Lock()

###############################################################################
# Function that is executed inside a thread. It runs one query at a time.
# The query is picked randomly from the provided pool of queries. If the query
# finishes faster than our expected baseline time, the thread will sleep.
###############################################################################


def runQueries(qPoolId):
    logging.debug("My query pool: %s", qPoolId)
    initialSleep = random.randint(0, targetRates[qPoolId]/2) # staggering
    logging.debug("initial sleep: %i", initialSleep)
    qPool = queryPools[qPoolId]
    conn = MySQLdb.connect(host='ccqserv125',
                           port=4040,
                           user='qsmaster',
                           passwd='',
                           db='LSST')
    cursor = conn.cursor()
    while (1):
        q = random.choice(qPool)
        logging.debug("QTYPE_%s START: Running: %s", qPoolId, q)
        startTime = time.time()
        #time.sleep(sleepTime[qPoolId])
        cursor.execute(q)
        rows = cursor.fetchall()
        outfile=os.path.join(OUTPUT_DIR, "%s_%s" % (qPoolId,threading.current_thread().ident))
        f = open(outfile, 'a')
        f.write("\n*************************************************\n")
        f.write("%s\n---\n" % q)
        for row in rows:
            for col in row:
                f.write("%s, " % col)
            f.write("\n")
        f.close()
        elT = time.time() - startTime            # elapsed
        # trying to run ~10% faster than the target rate
        loT = 0.9 * targetRates[qPoolId] - elT # left over
        logging.info('QTYPE_%s FINISHED: %s left %s %s', qPoolId, elT, loT, q)
        if loT < 0: # the query was slower than it should
            timeBehindMutex.acquire()
            timeBehind[qPoolId] -= loT
            logging.info("QTYPE_%s registering timeBehind %s, total is %s", qPoolId, loT, timeBehind[qPoolId])
            timeBehindMutex.release()
        elif timeBehind[qPoolId] > 0:
            timeBehindMutex.acquire()
            timeBehind[qPoolId] -= loT
            logging.info("QTYPE_%s recovering timeBehind %s, total is %s", qPoolId, loT, timeBehind[qPoolId])
            timeBehindMutex.release()
        else:
            logging.debug('QTYPE_%s sleeping %s', qPoolId, loT)
            time.sleep(loT)

###############################################################################
# Main. Starts all the threads. The threads will keep running for up to 24 h,
# or until the program gets interrupted (e.g. with Ctrl-C). Logging goes to a
# file in /tmp
###############################################################################

def main():
    logfile=os.path.join(OUTPUT_DIR, "qservMTest.log")
    logging.basicConfig(format="%(asctime)s %(thread)d: %(message)s",
                        filename=logfile,
                        level=logging.DEBUG)
    random.seed(123)

    for queryPoolId in queryPools:
        qCount = concurrency[queryPoolId]
        for i in range(0, qCount):
            t = threading.Thread(target=runQueries, args=(queryPoolId,))
            t.daemon = True
            t.start()
            t.join

    time.sleep(60*60*48)

if __name__ == "__main__":
    main()
