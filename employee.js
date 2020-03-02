const dbObj = require('./database');
const commObj = require('./common');
const empLeaveColl = "emp_leave";
const empBuffer = "emp_buffer";
const empProjColl = "emp_proj";
const locLeaveColl = "loc_holiday";


function computeWeekDaysInLeave(leaveArr) {
   let totalDays = 0;
   return new Promise(async (resolve, _reject) => {
      await leaveArr.forEach((leave) => {
         commObj.getDaysBetween(leave.startDate, leave.endDate, true).then((leaveDays) => {
            totalDays += leaveDays;
         });
      });
      resolve(totalDays);
   });
}

function computeAllDaysInLeave(leaveArr) {
   let totalDays = 0;
   return new Promise(async (resolve, _reject) => {
      await leaveArr.forEach((leave) => {
         commObj.getDaysBetween(leave.startDate, leave.endDate, false).then((leaveDays) => {
            totalDays += leaveDays;
         });
      });
      resolve(totalDays);
   });
}



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
            computeAllDaysInLeave(leaveArr).then((allDaysInLeave) => {
               computeWeekDaysInLeave(leaveArr).then((workDaysInLeave) => {
                  leaveArr.push({ 'totalDays': allDaysInLeave, 'workDays': workDaysInLeave });
               });
               resolve(leaveArr);
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



function getLocationLeave(wrkCity, revenueYear) {
   let revenueStart = new Date(revenueYear, 0, 2);
   revenueStart.setUTCHours(0, 0, 0, 0);
   let revenueEnd = new Date(revenueYear, 12, 1);
   revenueEnd.setUTCHours(0, 0, 0, 0);
   return new Promise((resolve, reject) => {
      dbObj.getDb().collection(locLeaveColl).aggregate([
         {
            $project: {
               "_id": 1,
               "wrkCity": 2,
               "startDate": 3,
               "endDate": 4,
               "days": 5,
               "description": 6,
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
               "wrkCity": { "$eq": wrkCity },
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
               "description": "$description"
            }
         }
      ]).toArray((err, locLeaveArr) => {
         if (err) {
            reject(err);
         } else if (locLeaveArr.length >= 1) {
            computeAllDaysInLeave(locLeaveArr).then((allDaysInLeave) => {
               computeWeekDaysInLeave(locLeaveArr).then((workDaysInLeave) => {
                  locLeaveArr.push({ 'totalDays': allDaysInLeave, 'workDays': workDaysInLeave });
               });
               resolve(locLeaveArr);
            });
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
                     getLocationLeave(empDtl[0].wrkCityCode, revenueYear).then((pubLeaveArr) => {
                        empDtl.push({ 'publicHolidays': pubLeaveArr });
                        resolve(empDtl);
                     });
                  }
               });
            });
         }
      });
   });
}



module.exports = {
   getPersonalLeave,
   getBuffer,
   getEmployeeProjection,
   getBufferCount,
   getLocationLeave
}