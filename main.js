const express = require("express");
const dbObj = require("./database");
const empObj = require("./employee");
const projObj = require("./project");
const commObj = require("./utility");
const libApp = express();
const port = 5454;
let bp = require('body-parser');
let cors = require('cors');
const { rejects } = require("assert");
const e = require("express");

libApp.use(bp.urlencoded({ extended: true }));
libApp.use(bp.json());
libApp.use(cors());

/* list projects */
libApp.get("/projectList", (_req, res) => {
    projObj.listAllProjects("main").then((projectList) => {
        res.json(projectList);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});


/* list employees in project */
libApp.get("/workforce", (req, res) => {
    let esaId = req.query.esaId;
    let revenueYear = req.query.revenueYear;
    empObj.listAssociates(esaId, revenueYear, "main").then((allEmpInProj) => {
        res.json(allEmpInProj);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

/* get one project revenue */
libApp.get("/projectRevenue", (req, res) => {
    let esaId = req.query.esaId;
    let revenueYear = req.query.revenueYear;
    projObj.getProjectRevenue(esaId, revenueYear, "main").then((projectRevenue) => {
        res.json(projectRevenue);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

/* get all project revenue */
libApp.get("/dashboard", (req, res) => {
    let revenueYear = req.query.revenueYear;
    projObj.getAllProjectRevenue(revenueYear, "main").then((projectRevenue) => {
        res.json(projectRevenue);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        res.json(errobj);
    });
});

/* get projection of one employee */
libApp.get("/employeeRevenue", (req, res) => {
    let revenueYear = req.query.revenueYear;
    let employeeFilter = req.query.employeeFilter;
    empObj.getProjection(revenueYear, employeeFilter, "main").then((empDtl) => {
        res.json(empDtl);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        return res.json(errobj);
    });
});

/* get projection of one employee */
libApp.get("/minMaxAllocYear", (req, res) => {
    let esaId = req.query.esaId;
    empObj.getMinMaxAllocationYear(esaId, "main").then((minMaxYear) => {
        res.json(minMaxYear);
    }).catch((err) => {
        errobj = { errcode: 500, error: err }
        return res.json(errobj);
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
    console.log("Error in starting server: " + error);
}