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


function getProjectRevenue(esaId, revenueYear) {
   let allEmpRevArr = [];
   let revenueArr = [];
   return new Promise((resolve, _reject) => {
      empObj.listEmployeeInProj(esaId).then((empInProj) => {
         empInProj.forEach((employee) => {
            let empObjId = employee._id.toString();
            allEmpRevArr.push(empObj.getEmployeeProjection(empObjId, revenueYear));
         });
         Promise.all(allEmpRevArr).then((empDtl) => {
            empDtl.forEach(async (empDtlObj, empIdx) => {
               await empDtlObj[4].revenue.forEach((empRev, mnthIdx) => {
                  let revenue = empRev.monthRevenue;
                  if (empIdx === 0) {
                     revenueArr.push({ "revenueMonth": empRev.month, "revenue": revenue });
                  } else {
                     let totalRevenue = revenueArr[mnthIdx].revenue;
                     totalRevenue += revenue;
                     revenueArr[mnthIdx].revenue = totalRevenue;
                  }
               })
            });
            resolve(revenueArr);
         });
      });
   });
}


module.exports = {
   listAllProjects,
   getProjectRevenue
}