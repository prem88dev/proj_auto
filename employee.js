const dbObj = require("./database");
const locObj = require("./location");
const commObj = require("./utility");
const revObj = require("./revenue");
const ObjectId = require('mongodb').ObjectID;
const dateFormat = require("dateformat");
const empLeaveColl = "emp_leave";
const empBuffer = "emp_buffer";
const empProjColl = "emp_proj";


function computeWeekdaysInLeave(leaveArr) {
   let weekdaysInLeave = 0;
   return new Promise(async (resolve, _reject) => {
      await leaveArr.forEach((leave) => {
         commObj.getDaysBetween(leave.startDate, leave.endDate, true).then((weekdays) => {
            weekdaysInLeave += weekdays;
         });
      });
      resolve(weekdaysInLeave);
   });
}

function computeLeaveDays(leaveArr) {
   let leaveDays = 0;
   return new Promise(async (resolve, _reject) => {
      await leaveArr.forEach((leave) => {
         commObj.getDaysBetween(leave.startDate, leave.endDate, false).then((daysBetween) => {
            leaveDays += daysBetween;
         });
      });
      resolve(leaveDays);
   });
}

function computeBufferDays(bufferArr) {
   let bufferDays = 0;
   return new Promise(async (resolve, _reject) => {
      await bufferArr.forEach((buffer) => {
         bufferDays += parseInt(buffer.days, 10);
      });
      resolve(bufferDays);
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
      if (empEsaLink === undefined || empEsaLink === "") {
         reject(getPersonalLeave.name + ": Linker ID is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(getPersonalLeave.name + ": Employee ID is not provided");
      } else if (revenueYear === undefined || revenueYear === "") {
         reject(getPersonalLeave.name + ": Revenue year is not provided");
      }
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
                  $dateFromParts: {
                     year: { $toInt: { $substr: ["$startDate", 4, -1] } },
                     month: { $toInt: { $substr: ["$startDate", 2, 2] } },
                     day: { $toInt: { $substr: ["$startDate", 0, 2] } },
                     hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
                  }
               },
               "leaveEnd": {
                  $dateFromParts: {
                     year: { $toInt: { $substr: ["$endDate", 4, -1] } },
                     month: { $toInt: { $substr: ["$endDate", 2, 2] } },
                     day: { $toInt: { $substr: ["$endDate", 0, 2] } },
                     hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
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
               "days": {
                  $switch: {
                     branches: [
                        { case: { "$eq": ["$leaveStart", "$leaveEnd"] }, "then": 1 },
                        {
                           case: { "$gte": ["$leaveStart", "$leaveEnd"] }, "then": {
                              $add: [{ $subtract: ["$leaveStart", "$leaveEnd"] }, 1]
                           }
                        }
                     ]
                  }
               },
               "reason": "$reason"
            }
         }
      ]).toArray((err, leaveArr) => {
         if (err) {
            reject("DB error in " + getPersonalLeave.name + ": " + err);
         } else if (leaveArr.length >= 1) {
            computeLeaveDays(leaveArr).then((allDaysInLeave) => {
               computeWeekdaysInLeave(leaveArr).then((workDaysInLeave) => {
                  leaveArr.push({ "totalDays": allDaysInLeave, "workDays": workDaysInLeave });
                  resolve(leaveArr);
               });
            });
         }
         else {
            resolve(leaveArr);
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
      if (empEsaLink === undefined || empEsaLink === "") {
         reject(getBuffer.name + ": Linker ID is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(getBuffer.name + ": Employee ID is not provided");
      } else if (revenueYear === undefined || revenueYear === "") {
         reject(getBuffer.name + ": Revenue year is not provided");
      }
      let bufferYear = parseInt(revenueYear, 10);
      let revenueStart = new Date(bufferYear, 0, 1);
      revenueStart.setUTCHours(0, 0, 0, 0);
      let revenueEnd = new Date(bufferYear, 12, 0);
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
                  $dateFromParts: {
                     year: { $toInt: { $substr: ["$month", 2, -1] } },
                     month: { $toInt: { $substr: ["$month", 0, 2] } },
                     day: 1, hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
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
      ]).toArray((err, bufferArr) => {
         if (err) {
            reject("DB error in " + getBuffer.name + "function: " + err);
         } else if (bufferArr.length >= 1) {
            computeBufferDays(bufferArr).then((bufferDays) => {
               bufferArr.push({ "totalDays": bufferDays });
               resolve(bufferArr);
            });
         }
         else {
            resolve(bufferArr);
         }
      });
   });
}

/* get projection data for specific employee in selected project */
function getEmployeeProjection(recordId, revenueYear) {
   return new Promise((resolve, reject) => {
      if (revenueYear === undefined || revenueYear === "") {
         reject(getEmployeeProjection.name + ": Revenue year is not provided");
      } else if (recordId === undefined || recordId === "") {
         reject(getEmployeeProjection.name + ": Record id is not provided");
      }

      let recordObjId = new ObjectId(recordId);
      let revenueStart = new Date(revenueYear, 0, 2);
      revenueStart.setUTCHours(0, 0, 0, 0);
      let revenueEnd = new Date(revenueYear, 12, 1);
      revenueEnd.setUTCHours(0, 0, 0, 0);
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
               localField: "cityCode",
               foreignField: "cityCode",
               as: "empEsaLoc"
            }
         },
         {
            $unwind: "$empEsaLoc"
         },
         {
            $match: {
               "_id": recordObjId
            }
         },
         {
            $project: {
               "_id": "$_id",
               "esaId": { $toInt: "$empEsaProj.esaId" },
               "esaDesc": "$empEsaProj.esaDesc",
               "projName": "$projName",
               "ctsEmpId": { $toInt: "$ctsEmpId" },
               "empFname": "$empFname",
               "empMname": "$empMname",
               "empLname": "$empLname",
               "lowesUid": "$lowesUid",
               "deptName": "$deptName",
               "sowStartDate": "$sowStartDate",
               "sowEndDate": "$sowEndDate",
               "foreseenEndDate": "$foreseenEndDate",
               "cityName": "$empEsaLoc.cityName",
               "cityCode": "$empEsaLoc.cityCode",
               "wrkHrPerDay": { $toInt: "$wrkHrPerDay" },
               "billRatePerHr": { $toInt: "$billRatePerHr" },
               "currency": "$empEsaProj.currency",
               "empEsaLink": "$empEsaLink",
               "projectionActive": { $toInt: "$projectionActive" }
            }
         }
      ]).toArray((err, empDtl) => {
         if (err) {
            reject("DB error in " + getEmployeeProjection.name + " function: " + err);
         } else if (empDtl.length === 1) {
            let empEsaLink = empDtl[0].empEsaLink;
            let ctsEmpId = `${empDtl[0].ctsEmpId}`;
            let cityCode = empDtl[0].cityCode;
            getPersonalLeave(empEsaLink, ctsEmpId, revenueYear).then((leaveArr) => {
               empDtl.push({ "leaves": leaveArr });
               getBuffer(empEsaLink, ctsEmpId, revenueYear).then((bufferArr) => {
                  empDtl.push({ "buffers": bufferArr });
                  locObj.getLocationLeave(cityCode, revenueYear).then((pubLeaveArr) => {
                     empDtl.push({ "publicHolidays": pubLeaveArr });
                     revObj.calcEmpRevenue(empDtl, revenueYear).then((revenueArr) => {
                        empDtl.push({ "revenue": revenueArr });
                        if (leaveArr.length === 0) {
                           empDtl[1].leaves.push("No leaves between " + dateFormat(revenueStart, "dd-mmm-yyyy") + " and " + dateFormat(revenueEnd, "dd-mmm-yyyy"));
                        }
                        if (bufferArr.length === 0) {
                           empDtl[2].buffers.push("No buffers between " + dateFormat(revenueStart, "dd-mmm-yyyy") + " and " + dateFormat(revenueEnd, "dd-mmm-yyyy"));
                        }
                        if (pubLeaveArr.length === 0) {
                           empDtl[3].publicHolidays.push("No location holidays between " + dateFormat(revenueStart, "dd-mmm-yyyy") + " and " + dateFormat(revenueEnd, "dd-mmm-yyyy"));
                        }
                        resolve(empDtl);
                     }).catch((calcEmpRevenueErr) => { reject(calcEmpRevenueErr); });
                  }).catch((getLocationLeaveErr) => { reject(getLocationLeaveErr); });
               }).catch((getBufferErr) => { reject(getBufferErr); });
            }).catch((getPersonalLeaveErr) => { reject(getPersonalLeaveErr); });
         } else if (empDtl.length === 0) {
            reject(getEmployeeProjection.name + ": No records found")
         } else if (empDtl.length > 1) {
            reject(getEmployeeProjection.name + ": More than one record found");
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
               "esaId": { "$first": { $toInt: "$empEsaProj.esaId" } },
               "esaDesc": { "$first": "$empEsaProj.esaDesc" },
               "projName": { "$first": "$projName" },
               "ctsEmpId": { "$first": { $toInt: "$ctsEmpId" } },
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" },
               "lowesUid": { "$first": "$lowesUid" },
               "deptName": { "$first": "$deptName" },
               "sowStartDate": { "$first": "$sowStartDate" },
               "sowEndDate": { "$first": "$sowEndDate" },
               "foreseenEndDate": { "$first": "$foreseenEndDate" },
               "wrkCity": { "$first": "$empEsaLoc.cityName" },
               "wrkHrPerDay": { "$first": { $toInt: "$wrkHrPerDay" } },
               "billRatePerHr": { "$first": { $toInt: "$billRatePerHr" } },
               "empEsaLink": { "$first": "$empEsaLink" },
               "projectionActive": { "$first": { $toInt: "$projectionActive" } },
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
            reject("DB error in " + listAllActiveEmployee.name + " function: " + err);
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
               "esaId": { "$first": { $toInt: "$empEsaProj.esaId" } },
               "esaDesc": { "$first": "$empEsaProj.esaDesc" },
               "projName": { "$first": "$projName" },
               "ctsEmpId": { "$first": { $toInt: "$ctsEmpId" } },
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" },
               "lowesUid": { "$first": "$lowesUid" },
               "deptName": { "$first": "$deptName" },
               "sowStartDate": { "$first": "$sowStartDate" },
               "sowEndDate": { "$first": "$sowEndDate" },
               "foreseenEndDate": { "$first": "$foreseenEndDate" },
               "wrkCity": { "$first": "$empEsaLoc.cityName" },
               "wrkHrPerDay": { "$first": { $toInt: "$wrkHrPerDay" } },
               "billRatePerHr": { "$first": { $toInt: "$billRatePerHr" } },
               "empEsaLink": { "$first": "$empEsaLink" },
               "projectionActive": { "$first": { $toInt: "$projectionActive" } },
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
            reject("DB error in " + listAllInactiveEmployee.name + " function: " + err);
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
            reject("DB error in " + listAllEmployees.name + " function: " + err);
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
            reject("DB error in " + getAllEmployeeLeaves.name + " function: " + err);
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
   listAllActiveEmployee,
   listAllInactiveEmployee,
   listAllEmployees,
   getAllEmployeeLeaves
}