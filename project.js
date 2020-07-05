const commObj = require("./utility");
const empObj = require("./employee");
const dateFormat = require("dateformat");


function addEmpRevenue(employeeObj, projectRevenue, callerName) {
   let funcName = addEmpRevenue.name;
   return new Promise(async (resolve, reject) => {
      if (projectRevenue === undefined || projectRevenue === "") {
         let projRevArr = [];
         await employeeObj[1].monthlyDetail.forEach((monthlyDetail) => {
            let revenueObj = monthlyDetail.revenue;
            projRevArr.push({ "revenueMonth": revenueObj.revenueMonth, "revenueAmount": revenueObj.revenueAmount, "cmiRevenueAmount": revenueObj.cmiRevenueAmount });
         });
         resolve(projRevArr);
      } else {
         if (projectRevenue.length > 0) {
            await employeeObj[1].monthlyDetail.forEach(async (monthlyDetail) => {
               let revenueObj = monthlyDetail.revenue;
               await projectRevenue.forEach((projRevDtl) => {
                  if (projRevDtl.revenueMonth === revenueObj.revenueMonth) {
                     projRevDtl.revenueAmount += revenueObj.revenueAmount;
                     projRevDtl.cmiRevenueAmount += revenueObj.cmiRevenueAmount;
                  }
               });
            });
         }
         resolve(projectRevenue);
      }
   });
}


function calcProjectRevenue(projectEmployee, projectRevenue, empDataIdx, callerName) {
   let funcName = calcProjectRevenue.name;
   return new Promise((resolve, reject) => {
      let empIdx = parseInt(empDataIdx, 10)
      if (empIdx === projectEmployee.length) {
         return resolve(projectRevenue);
      } else {
         addEmpRevenue(projectEmployee[empIdx], projectRevenue, funcName).then((projRev) => {
            let nextDataIdx = empIdx + 1;
            return resolve(calcProjectRevenue(projectEmployee, projRev, nextDataIdx, funcName));
         });
      }
   });
}


function getProjectRevenue(esaId, revenueYear, callerName) {
   let funcName = getProjectRevenue.name;
   return new Promise((resolve, reject) => {
      if (esaId === undefined || esaId === "") {
         reject(funcName + ": Project ID is not provided");
      } else if (revenueYear === undefined || revenueYear === "" || revenueYear.length !== 4) {
         reject(funcName + ": Revenue year is not provided");
      } else {
         let projectEmployee = [];
         empObj.getWorkforce(esaId, revenueYear, funcName).then(async (project) => {
            let workForce = project.workforce
            await workForce.forEach((employee) => {
               projectEmployee.push(empObj.getProjection(revenueYear, employee.employeeLinker, funcName)
                  .catch((getProjectionErr) => { reject(getProjectionErr) }));
            });

            if (projectEmployee.length >= 1) {
               Promise.all(projectEmployee).then(async (employeeList) => {
                  calcProjectRevenue(employeeList, "", 0, funcName).then((projectRevenue) => {
                     let tmpEmpObj = employeeList[0];
                     let prjRevArr = [];
                     if (projectRevenue.length > 0) {
                        projectRevenue.forEach((prjDtl) => {
                           prjRevArr.push({ "revenueMonth": prjDtl.revenueMonth, "revenueAmount": prjDtl.revenueAmount, "cmiRevenueAmount": prjDtl.cmiRevenueAmount });
                        });
                        employeeList.push({ "currency": tmpEmpObj[0].currency, "projectRevenue": prjRevArr });
                     } else {
                        employeeList.push({ "currency": tmpEmpObj[0].currency, "projectRevenue": projectRevenue });
                     }
                     resolve(employeeList);
                  });
               });
            } else {
               reject(funcName + ": No detail found !");
            }
         });
      }
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
      commObj.getProjectList(funcName).then((projectList) => {
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
   getProjectRevenue,
   getAllProjectRevenue
}