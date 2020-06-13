const dbObj = require('./database');
const empObj = require('./employee');
const dateFormat = require("dateformat");
const esaProjColl = "esa_proj";


/* get list of projects */
function listAllProjects(callerName) {
   return new Promise((resolve, reject) => {
      dbObj.getDb().collection(esaProjColl).aggregate([
         {
            $group: {
               "_id": "$esaId",
               "currency": { "$first": "$currency" },
               "billingMode": { "$first": "$billingMode" },
               "description": {
                  "$addToSet": {
                     "name": "$esaDesc",
                     "subType": "$esaSubType"
                  }
               }
            }
         },
         {
            $sort: { "_id": 1 }
         }
      ]).toArray(function (err, projectList) {
         if (err) {
            reject(err);
         } else {
            resolve(projectList);
         }
      });
   });
}


function addEmpRevenue(employeeRevenue, projectRevenue, callerName) {
   let funcName = addEmpRevenue.name;
   return new Promise(async (resolve, reject) => {
      if (projectRevenue === undefined || projectRevenue === "") {
         let projRevArr = [];
         await employeeRevenue.forEach((empRevDtl) => {
            projRevArr.push({ "revenueMonth": empRevDtl.revenueMonth, "revenueAmount": empRevDtl.revenueAmount, "cmiRevenueAmount": empRevDtl.cmiRevenueAmount });
         });
         resolve(projRevArr);
      } else {
         if (projectRevenue.length > 0) {
            await employeeRevenue.forEach(async (empRevDtl) => {
               await projectRevenue.forEach((projRevDtl) => {
                  if (projRevDtl.revenueMonth === empRevDtl.revenueMonth) {
                     projRevDtl.revenueAmount += empRevDtl.revenueAmount;
                     projRevDtl.cmiRevenueAmount += empRevDtl.cmiRevenueAmount;
                  }
               });
            });
         }
         resolve(projectRevenue);
      }
   });
}


function calcProjectRevenue(allEmpRevArr, projectRevenue, empDataIdx, callerName) {
   let funcName = calcProjectRevenue.name;
   return new Promise((resolve, reject) => {
      let empIdx = parseInt(empDataIdx, 10)
      if (empIdx === allEmpRevArr.length) {
         return resolve(projectRevenue);
      } else {
         addEmpRevenue(allEmpRevArr[empIdx][5].revenue, projectRevenue, funcName).then((projRev) => {
            let nextDataIdx = empIdx + 1;
            return resolve(calcProjectRevenue(allEmpRevArr, projRev, nextDataIdx, funcName));
         });
      }
   });
}


function getProjectRevenue(esaId, revenueYear, callerName) {
   let funcName = getProjectRevenue.name;
   return new Promise((resolve, reject) => {
      let empRevArr = [];
      empObj.listAssociates(esaId).then((empInProj) => {
         empInProj.forEach((employee) => {
            let empRecId = employee._id.toString();
            empRevArr.push(empObj.getProjection(empRecId, revenueYear, funcName));
         });
      }).then(() => {
         Promise.all(empRevArr).then((allEmpRevArr) => {
            calcProjectRevenue(allEmpRevArr, "", 0, funcName).then((projectRevenue) => {
               let prjRevArr = [];
               if (projectRevenue.length > 0) {
                  projectRevenue.forEach((prjDtl) => {
                     prjRevArr.push({ "revenueMonth": prjDtl.revenueMonth, "revenueAmount": prjDtl.revenueAmount, "cmiRevenueAmount": prjDtl.cmiRevenueAmount });
                  });
                  allEmpRevArr.push({ "projectRevenue": prjRevArr });
               } else {
                  allEmpRevArr.push({ "projectRevenue": projectRevenue });
               }
               resolve(allEmpRevArr);
            });
         });
      });
   });
}


function calcAnnualRevenue(dashboard, revenueYear, callerName) {
   let funcName = calcAnnualRevenue.name;
   let annualRevenue = [];
   return new Promise((resolve, reject) => {
      for (let monthIdx = 0; monthIdx <= 11; monthIdx++) {
         let searchMonth = dateFormat(new Date(revenueYear, monthIdx, 1), "mmm-yyyy");
         let revenueAmount = 0;
         let cmiRevenueAmount = 0;
         dashboard.forEach((project) => {
            let projMonthDtl = project.revenue;
            projMonthDtl.forEach((monthRevDtl) => {
               if (monthRevDtl.revenueMonth === searchMonth) {
                  revenueAmount += parseInt(monthRevDtl.revenueAmount, 10);
                  cmiRevenueAmount += parseInt(monthRevDtl.cmiRevenueAmount, 10);
               }
            });
         });
         annualRevenue.push({ "revenueMonth": searchMonth, "revenueAmount": revenueAmount, "cmiRevenueAmount": cmiRevenueAmount });
      }
      resolve(annualRevenue);
   });
}


function getAllProjectRevenue(revenueYear, callerName) {
   let funcName = getAllProjectRevenue.name;
   let allProjRevArr = [];
   let dashboard = [];
   return new Promise((resolve, reject) => {
      listAllProjects().then((projectList) => {
         projectList.forEach((project) => {
            allProjRevArr.push(getProjectRevenue(project._id, revenueYear, funcName));
         });
      }).then(() => {
         Promise.all(allProjRevArr).then((allProjRev) => {
            allProjRev.forEach((project) => {
               let projId = project[0][0].esaId;
               let currency = project[0][0].currency;
               let projRevArr = project[project.length - 1].projectRevenue;
               dashboard.push({ "esaId": projId, "currency": currency, "revenue": projRevArr });
            });
         }).then(async () => {
            await calcAnnualRevenue(dashboard, revenueYear).then((revenueGrandTotal) => {
               dashboard.push({ "esaId": "Grand Total", "revenue": revenueGrandTotal });
               resolve(dashboard);
            });
         });
      });
   });
}

module.exports = {
   listAllProjects,
   getProjectRevenue,
   getAllProjectRevenue
}