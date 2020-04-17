const express = require("express");
const dbObj = require("./database");
const empObj = require("./employee");
const projObj = require("./project");
const revObj = require("./revenue");
const libApp = express();
const port = 5454;
let bp = require('body-parser');
let cors = require('cors');

libApp.use(bp.urlencoded({ extended: true }));
libApp.use(bp.json());
libApp.use(cors());


/* get project wise trend */
libApp.get("/getLeave", (req, res) => {
    let noError = true;
    if (req.body.empEsaLink === undefined || req.body.empEsaLink == "") {
        res.json("Require empEsaLink");
        noError = false;
    } else if (req.body.ctsEmpId === undefined || req.body.ctsEmpId == "") {
        res.json("Require ctsEmpId");
        noError = false;
    } else if (req.body.revenueYear === undefined || req.body.revenueYear == "") {
        res.json("Require revenueYear");
        noError = false;
    } else if (req.body.monthLeaveIdc !== undefined && req.body.monthLeaveIdc != "") {
        if (req.body.monthIndex === undefined || req.body.monthIndex == "") {
            res.json("Require monthIndex [1 - 12]");
            noError = false;
        } else if (1 > parseInt(req.body.monthIndex, 10) || 12 < parseInt(req.body.monthIndex, 10)) {
            res.json("Incorrect month");
            noError = false;
        }

        if (noError === true) {
            let empEsaLink = req.body.empEsaLink;
            let ctsEmpId = req.body.ctsEmpId;
            let revenueYear = req.body.revenueYear;
            let monthIndex = req.body.monthIndex;
            empObj.getPersonalLeave(empEsaLink, ctsEmpId, revenueYear, monthIndex).then((leaves) => {
                res.json(leaves);
            }).catch((err) => {
                errobj = { errcode: 500, error: err }
                res.json(errobj);
            });
        }
    }
});


/* get project wise trend */
libApp.get("/getBuffer", (req, res) => {
    let empEsaLink = req.body.empEsaLink;
    let ctsEmpId = req.body.ctsEmpId;
    let revenueYear = req.body.revenueYear;
    empObj.getBuffer(empEsaLink, ctsEmpId, revenueYear).then((buffers) => {
        res.json(buffers);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

/* get project wise trend */
libApp.get("/projWiseTrend", (req, res) => {
    var esaId = req.body.esaId;
    var revenueYear = req.body.revenueYear;
    projObj.getProjectRevenue(esaId, revenueYear).then((projectRevenue) => {
        res.json(projectRevenue);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

/* list projects */
libApp.get("/listAllProj", (_req, res) => {
    projObj.listAllProjects().then((projectList) => {
        res.json(projectList);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

/* list employees in project */
libApp.get("/listEmpInProj", (req, res) => {
    var esaId = req.body.esaId;
    empObj.listEmployeeInProj(esaId).then((allEmpInProj) => {
        res.json(allEmpInProj);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

//Get all active employee projections of specific project
libApp.get("/getEmpDtl", (req, res) => {
    let recordId = req.body.recordId;
    let revenueYear = req.body.revenueYear;
    empObj.getEmployeeProjection(recordId, revenueYear).then((empDtl) => {
        res.json(empDtl);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

//get leave days of all associates
libApp.get("/getAllEmpLeave", (_req, res) => {
    dbObj.getAllEmployeeLeaves().then((allEmpLeave) => {
        res.json(allEmpLeave);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});


try {
    //initializing DB and start listening to port
    dbObj.initDb(() => {
        libApp.listen(port, function (err) {
            if (err) {
                throw err;
            }
            console.log("Server is up and running on port " + port);
        });
    });
} catch (error) {
    console.log("Error in starting server: ");
    console.log(error);
}