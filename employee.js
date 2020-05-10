const dbObj = require("./database");
const locObj = require("./location");
const commObj = require("./utility");
const revObj = require("./revenue");
const splWrkObj = require("./specialWorkday");
const ObjectId = require("mongodb").ObjectID;
const dateFormat = require("dateformat");
const empLeaveColl = "emp_leave";
const empBuffer = "emp_buffer";
const empProjColl = "emp_proj";
const mSecInDay = 86400000;


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
   listEmployeeInProj:
      to get list of employees in a project

   params:
      esaId - project id for which employees are to be listed
   
   returns array of employee with their first, middle and last names
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
            $match: {
               "esaId": esaId
            }
         },
         {
            $group: {
               "_id": "$_id",
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" }
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


function getYearlySelfLeaves(empEsaLink, ctsEmpId, revenueYear) {
   return new Promise((resolve, reject) => {
      let funcName = getYearlySelfLeaves.name;
      if (empEsaLink === undefined || empEsaLink === "") {
         reject(funcName + ": Linker ID is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(funcName + ": Employee ID is not provided");
      } else if (revenueYear === undefined || revenueYear === "") {
         reject(funcName + ": Revenue year is not provided");
      }
      let calcYear = parseInt(revenueYear, 10);
      let revenueStart = new Date(calcYear, 0, 2);
      revenueStart.setUTCHours(0, 0, 0, 0);
      let revenueStop = new Date(calcYear, 12, 1);
      revenueStop.setUTCHours(0, 0, 0, 0);
      dbObj.getDb().collection(empLeaveColl).aggregate([
         {
            $project: {
               "_id": "$_id",
               "esaId": "$esaId",
               "empEsaLink": "$empEsaLink",
               "ctsEmpId": "$ctsEmpId",
               "startDate": "$startDate",
               "endDate": "$endDate",
               "days": "$days",
               "reason": "$reason",
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
               $or: [
                  {
                     $and: [
                        { "leaveStart": { "$lte": revenueStart } },
                        { "leaveEnd": { "$gte": revenueStop } }
                     ]
                  },
                  {
                     $and: [
                        { "leaveStart": { "$lte": revenueStart } },
                        { "leaveEnd": { "$gte": revenueStart } },
                        { "leaveEnd": { "$lte": revenueStop } }
                     ]
                  },
                  {
                     $and: [
                        { "leaveStart": { "$gte": revenueStart } },
                        { "leaveStart": { "$lte": revenueStop } },
                        { "leaveEnd": { "$gte": revenueStart } },
                        { "leaveEnd": { "$lte": revenueStop } }
                     ]
                  },
                  {
                     $and: [
                        { "leaveStart": { "$gte": revenueStart } },
                        { "leaveStart": { "$lte": revenueStop } },
                        { "leaveEnd": { "$gte": revenueStart } },
                        { "leaveEnd": { "$gte": revenueStop } }
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
               "days": { $divide: [{ $subtract: ["$leaveEnd", "$leaveStart"] }, mSecInDay] },
               "reason": "$reason",
               "revenueStart": revenueStart,
               "revenueStop": revenueStop
            }
         },
         {
            $addFields: {
               startDate: {
                  $let: {
                     vars: {
                        monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                     },
                     in: {
                        $concat: [
                           { $toString: { $dayOfMonth: "$startDate" } }, "-",
                           { $arrayElemAt: ["$$monthsInString", { $month: "$startDate" }] }, "-",
                           { $toString: { $year: "$startDate" } }
                        ]
                     }
                  }
               },
               endDate: {
                  $let: {
                     vars: {
                        monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                     },
                     in: {
                        $concat: [
                           { $toString: { $dayOfMonth: "$endDate" } }, "-",
                           { $arrayElemAt: ["$$monthsInString", { $month: "$endDate" }] }, "-",
                           { $toString: { $year: "$endDate" } }
                        ]
                     }
                  }
               }
            }
         }
      ]).toArray((err, leaveArr) => {
         if (err) {
            reject("DB error in " + funcName + ": " + err);
         } else if (leaveArr.length >= 1) {
            commObj.countAllDays(leaveArr, funcName).then((allDaysInLeave) => {
               commObj.countWeekdays(leaveArr, funcName).then((workDaysInLeave) => {
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


function getSelfLeaveDates(empEsaLink, ctsEmpId, selfLeaveStart, selfLeaveStop, callerName) {
   return new Promise((resolve, reject) => {
      if (empEsaLink === undefined || empEsaLink === "") {
         reject(getSelfLeaveDates.name + ": Linker ID is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(getSelfLeaveDates.name + ": Employee ID is not provided");
      } else if (selfLeaveStart === undefined || selfLeaveStart === "") {
         reject(getSelfLeaveDates.name + ": Personal leave start date is not provided");
      } else if (selfLeaveStop === undefined || selfLeaveStop === "") {
         reject(getSelfLeaveDates.name + ": Personal leave stop date is not provided");
      } else {
         let refStartDate = new Date(selfLeaveStart);
         refStartDate.setUTCHours(0, 0, 0, 0);
         let refStopDate = new Date(selfLeaveStop);
         refStopDate.setUTCHours(0, 0, 0, 0);
         dbObj.getDb().collection(empLeaveColl).aggregate([
            {
               $project: {
                  "_id": 1,
                  "empEsaLink": 3,
                  "ctsEmpId": 4,
                  "startDate": 5,
                  "endDate": 6,
                  "leaveStart": {
                     $dateFromParts: {
                        year: { $toInt: { $substr: ["$startDate", 4, -1] } },
                        month: { $toInt: { $substr: ["$startDate", 2, 2] } },
                        day: { $toInt: { $substr: ["$startDate", 0, 2] } },
                        hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
                     }
                  },
                  "leaveStop": {
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
                  $or: [
                     {
                        $and: [
                           { "leaveStart": { $lte: refStartDate } },
                           { "leaveStop": { $gte: refStopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { $lte: refStartDate } },
                           { "leaveStop": { $gte: refStartDate } },
                           { "leaveStop": { $lte: refStopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { $gte: refStartDate } },
                           { "leaveStart": { $lte: refStopDate } },
                           { "leaveStop": { $gte: refStartDate } },
                           { "leaveStop": { $lte: refStopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { $gte: refStartDate } },
                           { "leaveStart": { $lte: refStopDate } },
                           { "leaveStop": { $gte: refStartDate } },
                           { "leaveStop": { $lte: refStopDate } }
                        ]
                     }
                  ]
               }
            },
            {
               $project: {
                  "_id": "$_id",
                  "funcName": getSelfLeaveDates.name,
                  "callerName": callerName,
                  "selfLeaveStart": "$leaveStart",
                  "selfLeaveStop": "$leaveEnd",
                  "days": { $divide: [{ $subtract: ["$leaveEnd", "$leaveStart"] }, mSecInDay] },
                  "refStartDate": refStartDate,
                  "refStopDate": refStopDate
               }
            }
         ]).toArray((err, selfLeaveArr) => {
            if (err) {
               reject("DB error in " + getSelfLeaveDates.name + ": " + err);
            } else {
               resolve(selfLeaveArr);
            }
         });
      }
   });
}


/*
   getBuffer:
      this function will return array of buffers

   params:
      input:
         empEsaLink - inker between employee and project
         ctsEmpId - employee id
         revenueYear - year for which leaves are required
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
      let calcYear = parseInt(revenueYear, 10);
      let revenueStart = new Date(calcYear, 0, 2);
      revenueStart.setUTCHours(0, 0, 0, 0);
      let revenueStop = new Date(calcYear, 12, 1);
      revenueStop.setUTCHours(0, 0, 0, 0);
      dbObj.getDb().collection(empBuffer).aggregate([
         {
            $project: {
               "_id": "$_id",
               "empEsaLink": "$empEsaLink",
               "ctsEmpId": "$ctsEmpId",
               "month": "$month",
               "days": "$days",
               "reason": "$reason",
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
               $and: [
                  { "empEsaLink": { "$eq": empEsaLink } },
                  { "ctsEmpId": { "$eq": ctsEmpId } },
                  { "bufferDate": { "$gte": revenueStart } },
                  { "bufferDate": { "$lte": revenueStop } }
               ]
            }
         },
         {
            $project: {
               "_id": "$_id",
               "month": "$month",
               "days": { $toInt: "$days" },
               "reason": "$reason",
               "revenueStart": revenueStart,
               "revenueStop": revenueStop
            }
         },
         {
            $addFields: {
               month: {
                  $let: {
                     vars: {
                        monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                     },
                     in: {
                        $concat: [
                           { $arrayElemAt: ["$$monthsInString", { $toInt: { $substr: ["$month", 0, 2] } }] }, "-",
                           { $substr: ["$month", 2, - 1] }
                        ]
                     }
                  }
               }
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

      let funcName = getEmployeeProjection.name;
      let recordObjId = new ObjectId(recordId);
      let intRevenueYear = parseInt(revenueYear, 10);
      let revenueStart = new Date(intRevenueYear, 0, 2);
      revenueStart.setUTCHours(0, 0, 0, 0);
      let revenueStop = new Date(intRevenueYear, 12, 1);
      revenueStop.setUTCHours(0, 0, 0, 0);
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
               "cityCode": "$empEsaLoc.cityCode",
               "cityName": "$empEsaLoc.cityName",
               "wrkHrPerDay": { $toInt: "$wrkHrPerDay" },
               "billRatePerHr": { $toInt: "$billRatePerHr" },
               "currency": "$empEsaProj.currency",
               "empEsaLink": "$empEsaLink",
               "revenueStart": revenueStart,
               "revenueStop": revenueStop
            }
         },
         {
            $addFields: {
               sowStartDate: {
                  $cond: {
                     if: { $ne: ["$sowStartDate", ""] }, then: {
                        $let: {
                           vars: {
                              monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                           },
                           in: {
                              $concat: [
                                 { $toString: { $toInt: { $substr: ["$sowStartDate", 0, 2] } } }, "-",
                                 { $arrayElemAt: ["$$monthsInString", { $toInt: { $substr: ["$sowStartDate", 2, 2] } }] }, "-",
                                 { $substr: ["$sowStartDate", 4, -1] }
                              ]
                           }
                        }
                     }, else: "$sowStartDate"
                  }
               },
               sowEndDate: {
                  $cond: {
                     if: { $ne: ["$sowEndDate", ""] }, then: {
                        $let: {
                           vars: {
                              monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                           },
                           in: {
                              $concat: [
                                 { $toString: { $toInt: { $substr: ["$sowEndDate", 0, 2] } } }, "-",
                                 { $arrayElemAt: ["$$monthsInString", { $toInt: { $substr: ["$sowEndDate", 2, 2] } }] }, "-",
                                 { $substr: ["$sowEndDate", 4, -1] }
                              ]
                           }
                        }
                     }, else: "$sowEndDate"
                  }
               },
               foreseenEndDate: {
                  $cond: {
                     if: { $ne: ["$foreseenEndDate", ""] }, then: {
                        $let: {
                           vars: {
                              monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                           },
                           in: {
                              $concat: [
                                 { $toString: { $toInt: { $substr: ["$foreseenEndDate", 0, 2] } } }, "-",
                                 { $arrayElemAt: ["$$monthsInString", { $toInt: { $substr: ["$foreseenEndDate", 2, 2] } }] }, "-",
                                 { $substr: ["$foreseenEndDate", 4, -1] }
                              ]
                           }
                        }
                     }, else: "$foreseenEndDate"
                  }
               }
            }
         }
      ]).toArray((err, empDtl) => {
         if (err) {
            reject("DB error in " + funcName + " function: " + err);
         } else if (empDtl.length === 1) {
            let empEsaLink = empDtl[0].empEsaLink;
            let strCtsEmpId = `${empDtl[0].ctsEmpId}`;
            let cityCode = empDtl[0].cityCode;
            getYearlySelfLeaves(empEsaLink, strCtsEmpId, intRevenueYear, funcName).then((selfLeaveArr) => {
               empDtl.push({ "leaves": selfLeaveArr });
               getBuffer(empEsaLink, strCtsEmpId, intRevenueYear, funcName).then((bufferArr) => {
                  empDtl.push({ "buffers": bufferArr });
                  locObj.getYearlyLocationLeaves(cityCode, intRevenueYear, funcName).then((locHolArr) => {
                     empDtl.push({ "publicHolidays": locHolArr });
                     splWrkObj.getSplWrkDays(empEsaLink, strCtsEmpId, cityCode, revenueStart, revenueStop, funcName).then((splWrkArr) => {
                        empDtl.push({ "specialWorkDays": splWrkArr });
                        revObj.calcEmpRevenue(empDtl, intRevenueYear).then((revenueArr) => {
                           empDtl.push({ "revenue": revenueArr });
                           if (selfLeaveArr.length === 0) {
                              empDtl[1].leaves.push("No leaves between " + dateFormat(revenueStart, "d-mmm-yyyy") + " and " + dateFormat(revenueStop, "d-mmm-yyyy"));
                           }
                           if (bufferArr.length === 0) {
                              empDtl[2].buffers.push("No buffers between " + dateFormat(revenueStart, "d-mmm-yyyy") + " and " + dateFormat(revenueStop, "d-mmm-yyyy"));
                           }
                           if (locHolArr.length === 0) {
                              empDtl[3].publicHolidays.push("No location holidays between " + dateFormat(revenueStart, "d-mmm-yyyy") + " and " + dateFormat(revenueStop, "d-mmm-yyyy"));
                           }
                           resolve(empDtl);
                        }).catch((calcEmpRevenueErr) => { reject(calcEmpRevenueErr); });
                     }).catch((getSplWrkDaysErr) => { reject(getSplWrkDaysErr); });
                  }).catch((getYearlyLocationLeavesErr) => { reject(getYearlyLocationLeavesErr); });
               }).catch((getBufferErr) => { reject(getBufferErr); });
            }).catch((getYearlySelfLeavesErr) => { reject(getYearlySelfLeavesErr); });
         } else if (empDtl.length === 0) {
            reject(funcName + ": No records found")
         } else if (empDtl.length > 1) {
            reject(funcName + ": More than one record found");
         }
      });
   });
}




module.exports = {
   listEmployeeInProj,
   getYearlySelfLeaves,
   getSelfLeaveDates,
   getBuffer,
   getEmployeeProjection
}