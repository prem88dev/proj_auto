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


function addEmpRevenue(employeeRevenue, projectRevenue) {
   return new Promise(async (resolve, _reject) => {
      if (projectRevenue === undefined || projectRevenue === "") {
         resolve(employeeRevenue);
      } else if (projectRevenue.length > 0) {
         await employeeRevenue.forEach(async (empRevDtl) => {
            await projectRevenue.forEach((projRevDtl) => {
               if (projRevDtl.month === empRevDtl.month) {
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


function calcProjectRevenue(allEmpRevArr, projectRevenue, empDataIdx) {
   return new Promise((resolve, _reject) => {
      if (parseInt(empDataIdx, 10) === allEmpRevArr.length) {
         return resolve(projectRevenue);
      } else {
         addEmpRevenue(allEmpRevArr[empDataIdx][5].revenue, projectRevenue).then((projRev) => {
            let nextDataIdx = parseInt(empDataIdx, 10) + 1;
            return resolve(calcProjectRevenue(allEmpRevArr, projRev, nextDataIdx));
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
            calcProjectRevenue(allEmpRevArr, "", 0).then((projectRevenue) => {
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