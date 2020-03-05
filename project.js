const dbObj = require('./database');
const empObj = require('./employee');
const empProjColl = "emp_proj";
const esaProjColl = "esa_proj";


/* get list of projects */
function listAllProjects() {
   return new Promise((resolve, reject) => {
      db = dbObj.getDb();
      db.collection(esaProjColl).aggregate([
         {
            $project: {
               "_id": 1,
               "esaId": 2,
               "esaDesc": 3,
               "currency": 4,
               "billingMode": 5,
               "empEsaLink": 6
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


/*
   listEmployeeInProj:
      to get list of employees in a project

   params:
      empEsaLink - linker between employee and project
      ctsEmpId - employee id
      revenueYear - year for which leaves are required
      personal - boolean flag
         if true, will fetch personal leaves
         else, will fetch location holidays

   returns an array with leave details
*/
function listEmployeeInProj(esaId) {
   return new Promise((resolve, reject) => {
      db = dbObj.getDb();
      db.collection(empProjColl).aggregate([
         {
            $lookup: {
               from: "esa_proj",
               localField: "empEsaLink",
               foreignField: "empEsaLink",
               as: "empEsaProj"
            }
         },
         {
            $unwind: "$empEsaProj"
         },
         {
            $lookup: {
               from: "wrk_loc",
               localField: "wrkCity",
               foreignField: "wrkCity",
               as: "empEsaLoc"
            }
         },
         {
            $unwind: "$empEsaLoc"
         },
         {
            $match: {
               "esaId": esaId
            }
         },
         {
            $project: {
               "_id": 1,
               "esaId": 2,
               "esaDesc": 3,
               "projName": 4,
               "ctsEmpId": 5,
               "empFname": 6,
               "empMname": 7,
               "empLname": 8,
               "lowesUid": 9,
               "deptName": 10,
               "sowStartDate": 11,
               "sowEndDate": 12,
               "foreseenEndDate": 13,
               "wrkCity": 14,
               "wrkHrPerDay": 15,
               "billRatePerHr": 16,
               "empEsaLink": 17,
               "projectionActive": 18
            }
         }
      ]).toArray(function (err, allProj) {
         if (err) {
            reject(err);
         } else {
            resolve(allProj);
         }
      });
   });
}


function getProjectRevenue(esaId, revenueYear) {
   let empDtlArr = [];
   return new Promise((resolve, _reject) => {
      listEmployeeInProj(esaId).then((empInProj) => {
         empInProj.forEach((employee) => {
            let empObjId = employee._id.toString();
            empDtlArr.push(empObj.getEmployeeProjection(empObjId, revenueYear));
         });
         Promise.all(empDtlArr).then((empDtl) => {
            resolve(empDtl);
         })
      });
   });
}


//get projection data for all projects
function listInactiveEmployeeInProj(esaId) {
   return new Promise((resolve, reject) => {
      db = getDb();
      var myCol = db.collection(empProjColl);
      myCol.aggregate([
         {
            $lookup: {
               from: "esa_proj",
               localField: "empEsaLink",
               foreignField: "empEsaLink",
               as: "empEsaProj"
            }
         },
         {
            $unwind: "$empEsaProj"
         },
         {
            $lookup: {
               from: "wrk_loc",
               localField: "wrkCity",
               foreignField: "wrkCity",
               as: "empEsaLoc"
            }
         },
         {
            $unwind: "$empEsaLoc"
         },
         {
            $lookup: {
               from: "emp_leave",
               localField: "ctsEmpId",
               foreignField: "ctsEmpId",
               as: "empEsaLeave"
            }
         },
         {
            $unwind: "$empEsaLeave"
         },
         {
            $match: {
               "esaId": esaId,
               "projectionActive": "0"
            }
         },
         {
            $group: {
               "_id": "$_id",
               "esaId": { "$first": "$empEsaProj.esaId" },
               "esaDesc": { "$first": "$empEsaProj.esaDesc" },
               "projName": { "$first": "$projName" },
               "ctsEmpId": { "$first": "$ctsEmpId" },
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" },
               "lowesUid": { "$first": "$lowesUid" },
               "deptName": { "$first": "$deptName" },
               "sowStartDate": { "$first": "$sowStartDate" },
               "sowEndDate": { "$first": "$sowEndDate" },
               "foreseenEndDate": { "$first": "$foreseenEndDate" },
               "wrkCity": { "$first": "$empEsaLoc.cityName" },
               "wrkHrPerDay": { "$first": "$wrkHrPerDay" },
               "billRatePerHr": { "$first": "$billRatePerHr" },
               "empEsaLink": { "$first": "$empEsaLink" },
               "projectionActive": { "$first": "$projectionActive" },
               "leave": {
                  "$push": {
                     "_id": "$empEsaLeave._id",
                     "month": "$empEsaLeave.month",
                     "startDate": "$empEsaLeave.startDate",
                     "endDate": "$empEsaLeave.endDate",
                     "days": "$empEsaLeave.days"
                  }
               }
            }
         }
      ]).toArray(function (err, oneProj) {
         if (err) {
            reject(err);
         } else {
            resolve(oneProj);
         }
      });
   });
}



//get projection data for all projects
function listActiveEmployeeInProj(esaId) {
   return new Promise((resolve, reject) => {
      db = getDb();
      var myCol = db.collection(empProjColl);
      myCol.aggregate([
         {
            $lookup: {
               from: "esa_proj",
               localField: "empEsaLink",
               foreignField: "empEsaLink",
               as: "empEsaProj"
            }
         },
         {
            $unwind: "$empEsaProj"
         },
         {
            $lookup: {
               from: "wrk_loc",
               localField: "wrkCity",
               foreignField: "wrkCity",
               as: "empEsaLoc"
            }
         },
         {
            $unwind: "$empEsaLoc"
         },
         {
            $lookup: {
               from: "emp_leave",
               localField: "ctsEmpId",
               foreignField: "ctsEmpId",
               as: "empEsaLeave"
            }
         },
         {
            $unwind: "$empEsaLeave"
         },
         {
            $match: {
               "esaId": esaId,
               "projectionActive": "1"
            }
         },
         {
            $group: {
               "_id": "$_id",
               "esaId": { "$first": "$empEsaProj.esaId" },
               "esaDesc": { "$first": "$empEsaProj.esaDesc" },
               "projName": { "$first": "$projName" },
               "ctsEmpId": { "$first": "$ctsEmpId" },
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" },
               "lowesUid": { "$first": "$lowesUid" },
               "deptName": { "$first": "$deptName" },
               "sowStartDate": { "$first": "$sowStartDate" },
               "sowEndDate": { "$first": "$sowEndDate" },
               "foreseenEndDate": { "$first": "$foreseenEndDate" },
               "wrkCity": { "$first": "$empEsaLoc.cityName" },
               "wrkHrPerDay": { "$first": "$wrkHrPerDay" },
               "billRatePerHr": { "$first": "$billRatePerHr" },
               "empEsaLink": { "$first": "$empEsaLink" },
               "projectionActive": { "$first": "$projectionActive" },
               "leave": {
                  "$push": {
                     "_id": "$empEsaLeave._id",
                     "month": "$empEsaLeave.month",
                     "startDate": "$empEsaLeave.startDate",
                     "endDate": "$empEsaLeave.endDate",
                     "days": "$empEsaLeave.days"
                  }
               }
            }
         }
      ]).toArray(function (err, oneProj) {
         if (err) {
            reject(err);
         } else {
            resolve(oneProj);
         }
      });
   });
}



module.exports = {
   listAllProjects,
   listEmployeeInProj,
   getProjectRevenue,
   listInactiveEmployeeInProj,
   listActiveEmployeeInProj
}