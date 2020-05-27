const dbObj = require('./database');
const empObj = require('./employee');
const esaProjColl = "esa_proj";


/* get list of projects */
function listAllProjects() {
   return new Promise((resolve, reject) => {
      db = dbObj.getDb();
      db.collection(esaProjColl).aggregate([
         {
            $project: {
               "_id": "$_id",
               "esaId": { $toInt: "$esaId" },
               "esaDesc": "$esaDesc",
               "currency": "$currency",
               "billingMode": "$billingMode",
               "empEsaLink": "$empEsaLink"
            }
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
   return new Promise(async (resolve, _reject) => {
      if (projectRevenue === undefined || projectRevenue === "") {
         let projRevArr = [];
         await employeeRevenue.forEach((empRevDtl) => {
            projRevArr.push({"revenueMonth": empRevDtl.revenueMonth, "revenueAmount": empRevDtl.revenueAmount, "cmiRevenueAmount": empRevDtl.cmiRevenueAmount});
         });
         resolve(projRevArr);
      } else if (projectRevenue.length > 0) {
         await employeeRevenue.forEach(async (empRevDtl) => {
            await projectRevenue.forEach((projRevDtl) => {
               if (projRevDtl.revenueMonth === empRevDtl.revenueMonth) {
                  projRevDtl.revenueAmount += empRevDtl.revenueAmount;
                  projRevDtl.cmiRevenueAmount += empRevDtl.cmiRevenueAmount;
               }
            });
         });
         resolve(projectRevenue);
      } else {
         resolve(projectRevenue);
      }
   });
}


function calcProjectRevenue(allEmpRevArr, projectRevenue, empDataIdx, callerName) {
   let funcName = calcProjectRevenue.name;
   return new Promise((resolve, _reject) => {
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
   let empRevArr = [];
   return new Promise((resolve, _reject) => {
      empObj.listAssociates(esaId).then((empInProj) => {
         empInProj.forEach((employee) => {
            let empObjId = employee._id.toString();
            empRevArr.push(empObj.getProjection(empObjId, revenueYear, funcName));
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


module.exports = {
   listAllProjects,
   getProjectRevenue
}