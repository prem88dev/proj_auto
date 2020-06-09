const dbObj = require('./database');
const empObj = require('./employee');
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


function getAllProjectRevenue(revenueYear, callerName) {
   let funcName = getAllProjectRevenue.name;
   let allProjRevArr = [];
   let dashboard = [];
   return new Promise((resolve, reject) => {
      listAllProjects().then(async (projectList) => {
         await projectList.forEach((project) => {
            allProjRevArr.push(getProjectRevenue(project._id, revenueYear, funcName));
         });
         Promise.all(allProjRevArr).then((allProjRev) => {
            allProjRev.forEach((project) => {
               let projId = project[0][0].esaId;
               let revArr = project[project.length - 1].projectRevenue;
               dashboard.push({ "esaId": projId, "revenue": revArr });
            });
            resolve(dashboard);
         });
      });
   });
}

module.exports = {
   listAllProjects,
   getProjectRevenue,
   getAllProjectRevenue
}