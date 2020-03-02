const express = require('express');
const dbObj = require('./database');
const empObj = require('./employee');
const libApp = express();
const port = 5454;
let bp = require('body-parser');
let cors = require('cors');

libApp.use(bp.urlencoded({ extended: true }));
libApp.use(bp.json());
libApp.use(cors());


libApp.get("/countLeaves", (req, res) => {
    empObj.getLeaveCount(req.body.empEsaLink, req.body.ctsEmpId).then((count) => {
        res.json(count);
    })
});


libApp.get("/countBuffers", (req, res) => {
    empObj.getBufferCount(req.body.empEsaLink, req.body.ctsEmpId).then((count) => {
        res.json(count);
    })
});

libApp.get("/getEmpLeave", (req, res) => {
    empObj.getPersonalLeave(req.body.empEsaLink, req.body.ctsEmpId, req.body.revenueYear).then((count) => {
        res.json(count);
    })
});

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
    let monthIndex = req.body.monthIndex;
    let monthBufferIdc = req.body.monthBufferIdc;
    empObj.getBuffer(empEsaLink, ctsEmpId, revenueYear, monthIndex, monthBufferIdc).then((buffers) => {
        res.json(buffers);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});


/* get project wise trend */
libApp.get("/getLocLeave", (req, res) => {
    let wrkCity = req.body.wrkCity;
    let revenueYear = req.body.revenueYear;
    let monthIndex = req.body.monthIndex;
    empObj.getLocationLeave(wrkCity, revenueYear, monthIndex).then((locLeaves) => {
        res.json(locLeaves);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

/* get project wise trend */
libApp.get("/projWiseTrend", (req, res) => {
    var esaId = req.body.esaId;
    var revenueYear = req.body.revenueYear;
    dbObj.getProjectRevenue(esaId, revenueYear).then((projectRevenue) => {
        res.json(projectRevenue);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

/* list projects */
libApp.get("/listAllProj", (_req, res) => {
    dbObj.listAllProjects().then((projectList) => {
        res.json(projectList);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

/* list employees in project */
libApp.get("/listEmpInProj", (req, res) => {
    var esaId = req.body.esaId;
    dbObj.listEmployeeInProj(esaId).then((allEmpInProj) => {
        res.json(allEmpInProj);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

//Get all active employee projections of specific project
libApp.get("/getEmpDtl", (req, res) => {
    var empEsaLink = req.body.empEsaLink;
    var ctsEmpId = req.body.ctsEmpId;
    var revenueYear = req.body.revenueYear;
    empObj.getEmployeeProjection(empEsaLink, ctsEmpId, revenueYear).then((empDtl) => {
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

//list projection for all associates across all projects
libApp.get("/listAllEmp", (_req, res) => {
    dbObj.listAllEmployees().then((allEmp) => {
        res.json(allEmp);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

//list all active projections across all projects
libApp.get("/listAllActEmp", (_req, res) => {
    dbObj.listAllActiveEmployee().then((allActEmp) => {
        res.json(allActEmp);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

//list all inactive projections across all projects
libApp.get("/listAllInactEmp", (_req, res) => {
    dbObj.listAllInactiveEmployee().then((allInactEmp) => {
        res.json(allInactEmp);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

//list all active projection for associates in a specific project
libApp.get("/listActEmpInProj", (req, res) => {
    var esaId = req.body.esaId;
    dbObj.listActiveEmployeeInProj(esaId).then((actEmpInProj) => {
        res.json(actEmpInProj);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

//list all inactive projection associates in a specific project
libApp.get("/listInactEmpInProj", (req, res) => {
    var esaId = req.body.esaId;
    dbObj.listInactiveEmployeeInProj(esaId).then((inactInProj) => {
        res.json(inactInProj);
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