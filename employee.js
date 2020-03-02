const dbObj = require('./database');
const locObj = require('./location');
const commObj = require('./utility');
const revObj = require('./revenue');
const empLeaveColl = "emp_leave";
const empBuffer = "emp_buffer";
const empProjColl = "emp_proj";


function getBufferCount(empEsaLink, ctsEmpId) {
   let revenueStart = new Date(2020, 0, 2);
   revenueStart.setUTCHours(0, 0, 0, 0);
   let revenueEnd = new Date(2020, 12, 1);
   revenueEnd.setUTCHours(0, 0, 0, 0);
   return new Promise((resolve, reject) => {
      dbObj.getDb().collection(empBuffer).aggregate([
         {
            $project: {
               "_id": 1,
               "empEsaLink": 3,
               "ctsEmpId": 4,
               "month": 5,
               "days": 6,
               "bufferDate": {
                  $dateFromString: {
                     dateString: { "$concat": ["01", "$month"] },
                     format: "%d%m%Y"
                  }
               }
            }
         },
         {
            $match: {
               "$and": [
                  { "empEsaLink": empEsaLink },
                  { "ctsEmpId": ctsEmpId },
                  { "bufferDate": { "$gte": revenueStart } },
                  { "bufferDate": { "$lte": revenueEnd } }
               ]
            }
         },
         {
            $group: {
               "_id": null,
               "totalDays": { $sum: { $toInt: "$days" } }
            }
         }
      ]).toArray((err, totalDays) => {
         if (err) {
            reject(err);
         } else {
            resolve(totalDays);
         }
      });
   });
}

/*
   getPersonalLeave:
      returns an array of personal leaves

   params:
      input:
         empEsaLink - linker between employee and project
         ctsEmpId - employee id
         revenueYear - year for which leaves are required
         monthIndex - month for which leave are required
*/
function getPersonalLeave(empEsaLink, ctsEmpId, revenueYear) {
   return new Promise((resolve, reject) => {
      let leaveYear = parseInt(revenueYear, 10);
      let revenueStart = new Date(leaveYear, 0, 2);
      revenueStart.setUTCHours(0, 0, 0, 0);
      let revenueEnd = new Date(leaveYear, 12, 1);
      revenueEnd.setUTCHours(0, 0, 0, 0);
      dbObj.getDb().collection(empLeaveColl).aggregate([
         {
            $project: {
               "_id": 1,
               "esaId": 2,
               "empEsaLink": 3,
               "ctsEmpId": 4,
               "startDate": 5,
               "endDate": 6,
               "days": 7,
               "reason": 8,
               "leaveStart": {
                  $dateFromString: {
                     dateString: "$startDate",
                     format: "%d%m%Y"
                  }
               },
               "leaveEnd": {
                  $dateFromString: {
                     dateString: "$endDate",
                     format: "%d%m%Y"
                  }
               }
            }
         },
         {
            $match: {
               "empEsaLink": empEsaLink,
               "ctsEmpId": ctsEmpId,
               "$or": [
                  {
                     "$and": [
                        { "leaveStart": { "$lte": revenueStart } },
                        { "leaveEnd": { "$gte": revenueEnd } }
                     ]
                  },
                  {
                     "$and": [
                        { "leaveStart": { "$lte": revenueStart } },
                        { "leaveEnd": { "$gte": revenueStart } },
                        { "leaveEnd": { "$lte": revenueEnd } }
                     ]
                  },
                  {
                     "$and": [
                        { "leaveStart": { "$gte": revenueStart } },
                        { "leaveStart": { "$lte": revenueEnd } },
                        { "leaveEnd": { "$gte": revenueStart } },
                        { "leaveEnd": { "$lte": revenueEnd } }
                     ]
                  }
               ]
            }
         },
         {
            $project: {
               "_id": "$_id",
               "startDate": "$leaveStart",
               "endDate": "$leaveEnd",
               "days": "$days",
               "reason": "$reason"
            }
         }
      ]).toArray((err, leaveArr) => {
         if (err) {
            reject(err);
         } else if (leaveArr.length >= 1) {
            commObj.computeLeaveDays(leaveArr).then((allDaysInLeave) => {
               commObj.computeWeekdaysInLeave(leaveArr).then((workDaysInLeave) => {
                  leaveArr.push({ 'totalDays': allDaysInLeave, 'workDays': workDaysInLeave });
                  resolve(leaveArr);
               });
            });
         }
         else {
            resolve("No leaves between " + dateFormat(revenueStart, "dd-mmm-yyyy") + " and " + dateFormat(revenueEnd, "dd-mmm-yyyy"));
         }
      });
   });
}

/*
   getEmployeeLeave:
      this function will return array of either personal
      or location leaves based on personal flag

   params:
      input:
         empEsaLink - inker between employee and project
         ctsEmpId - employee id
         revenueYear - year for which leaves are required
      return an arry of leaves for the given revenue year
*/
function getBuffer(empEsaLink, ctsEmpId, revenueYear) {
   return new Promise((resolve, reject) => {
      let bufferYear = parseInt(revenueYear, 10);
      let revenueStart = new Date(bufferYear, 0, 2);
      revenueStart.setUTCHours(0, 0, 0, 0);
      let revenueEnd = new Date(bufferYear, 12, 1);
      revenueEnd.setUTCHours(0, 0, 0, 0);
      dbObj.getDb().collection(empBuffer).aggregate([
         {
            $project: {
               "_id": 1,
               "empEsaLink": 3,
               "ctsEmpId": 4,
               "month": 5,
               "days": 6,
               "reason": 7,
               "bufferDate": {
                  $dateFromString: {
                     dateString: { "$concat": ["01", "$month"] },
                     format: "%d%m%Y"
                  }
               }
            }
         },
         {
            $match: {
               "$and": [
                  { "empEsaLink": { "$eq": empEsaLink } },
                  { "ctsEmpId": { "$eq": ctsEmpId } },
                  { "bufferDate": { "$gte": revenueStart } },
                  { "bufferDate": { "$lte": revenueEnd } }
               ]
            }
         },
         {
            $project: {
               "_id": "$_id",
               "month": "$month",
               "days": "$days",
               "reason": "$reason"
            }
         }
      ]).toArray((err, buffArr) => {
         if (err) {
            reject(err);
         } else if (buffArr.length >= 1) {
            getBufferCount(empEsaLink, ctsEmpId, revenueStart, revenueEnd).then(async (days) => {
               await days.forEach((day) => {
                  buffArr.push(day);
               });
               resolve(buffArr);
            });
         }
         else {
            resolve("No buffers between " + dateFormat(revenueStart, "dd-mmm-yyyy") + " and " + dateFormat(revenueEnd, "dd-mmm-yyyy"));
         }
      });
   });
}

/* get projection data for specific employee in selected project */
function getEmployeeProjection(empEsaLink, ctsEmpId, revenueYear) {
   let revenueStart = new Date(revenueYear, 0, 2);
   revenueStart.setUTCHours(0, 0, 0, 0);
   let revenueEnd = new Date(revenueYear, 12, 1);
   revenueEnd.setUTCHours(0, 0, 0, 0);
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
               "empEsaLink": empEsaLink,
               "ctsEmpId": ctsEmpId
            }
         },
         {
            $project: {
               "_id": "$_id",
               "esaId": "$empEsaProj.esaId",
               "esaDesc": "$empEsaProj.esaDesc",
               "projName": "$projName",
               "ctsEmpId": "$ctsEmpId",
               "empFname": "$empFname",
               "empMname": "$empMname",
               "empLname": "$empLname",
               "lowesUid": "$lowesUid",
               "deptName": "$deptName",
               "sowStartDate": "$sowStartDate",
               "sowEndDate": "$sowEndDate",
               "foreseenEndDate": "$foreseenEndDate",
               "wrkCity": "$empEsaLoc.cityName",
               "wrkCityCode": "$empEsaLoc.wrkCity",
               "wrkHrPerDay": "$wrkHrPerDay",
               "billRatePerHr": "$billRatePerHr",
               "empEsaLink": "$empEsaLink",
               "projectionActive": "$projectionActive"
            }
         }
      ]).toArray((err, empDtl) => {
         if (err) {
            reject(err);
         } else if (empDtl.length === 1) {
            getPersonalLeave(empEsaLink, ctsEmpId, revenueYear).then((leaveArr) => {
               empDtl.push({ 'leaves': leaveArr });
               getBuffer(empEsaLink, ctsEmpId, revenueYear).then((bufferArr) => {
                  empDtl.push({ 'buffers': bufferArr });
                  if (empDtl[0].wrkCityCode != "") {
                     locObj.getLocationLeave(empDtl[0].wrkCityCode, revenueYear).then((pubLeaveArr) => {
                        empDtl.push({ 'publicHolidays': pubLeaveArr });
                        revObj.calcEmpRevenue(empDtl, revenueYear).then((revenueArr) => {
                           empDtl.push({'revenue': revenueArr});
                           resolve(empDtl);
                        });
                     });
                  }
               });
            });
         } else {
            resolve("More than one record found !");
         }
      });
   });
}

/* get projection data for all projects */
function listAllActiveEmployee() {
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

/* get projection data for all projects */
function listAllInactiveEmployee() {
   return new Promise((resolve, reject) => {
      db = getDb();
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

/* get projection data for all projects */
function listAllEmployees() {
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

/* get leave days of all Employees */
function getAllEmployeeLeaves() {
   return new Promise((resolve, reject) => {
      db = getDb();
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
            $group: {
               "_id": "$_id",
               "esaId": { "$first": "$empEsaProj.esaId" },
               "esaDesc": { "$first": "$empEsaProj.esaDesc" },
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
   getPersonalLeave,
   getBuffer,
   getEmployeeProjection,
   getBufferCount,
   listAllActiveEmployee,
   listAllInactiveEmployee,
   listAllEmployees,
   getAllEmployeeLeaves
}