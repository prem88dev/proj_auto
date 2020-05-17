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
   listAssociates:
      to get list of employees in a project

   params:
      esaId - project id for which employees are to be listed
   
   returns array of employee with their first, middle and last names
*/
function listAssociates(esaId) {
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
               "stopDate": "$stopDate",
               "halfDay": "$halfDay",
               "reason": "$reason",
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
                     year: { $toInt: { $substr: ["$stopDate", 4, -1] } },
                     month: { $toInt: { $substr: ["$stopDate", 2, 2] } },
                     day: { $toInt: { $substr: ["$stopDate", 0, 2] } },
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
                        { "leaveStop": { "$gte": revenueStop } }
                     ]
                  },
                  {
                     $and: [
                        { "leaveStart": { "$lte": revenueStart } },
                        { "leaveStop": { "$gte": revenueStart } },
                        { "leaveStop": { "$lte": revenueStop } }
                     ]
                  },
                  {
                     $and: [
                        { "leaveStart": { "$gte": revenueStart } },
                        { "leaveStart": { "$lte": revenueStop } },
                        { "leaveStop": { "$gte": revenueStart } },
                        { "leaveStop": { "$lte": revenueStop } }
                     ]
                  },
                  {
                     $and: [
                        { "leaveStart": { "$gte": revenueStart } },
                        { "leaveStart": { "$lte": revenueStop } },
                        { "leaveStop": { "$gte": revenueStart } },
                        { "leaveStop": { "$gte": revenueStop } }
                     ]
                  }
               ]
            }
         },
         {
            $project: {
               "_id": "$_id",
               "startDate": "$leaveStart",
               "stopDate": "$leaveStop",
               "days": {
                  $cond: {
                     if: { $eq: ["$halfDay", "Y"] }, then: {
                        $divide: [{ $add: [{ $subtract: ["$leaveStop", "$leaveStart"] }, mSecInDay] }, (mSecInDay * 2)]
                     },
                     else: { $divide: [{ $add: [{ $subtract: ["$leaveStop", "$leaveStart"] }, mSecInDay] }, mSecInDay] }
                  }
               },
               "halfDay": "$halfDay",
               "reason": "$reason"
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
               stopDate: {
                  $let: {
                     vars: {
                        monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                     },
                     in: {
                        $concat: [
                           { $toString: { $dayOfMonth: "$stopDate" } }, "-",
                           { $arrayElemAt: ["$$monthsInString", { $month: "$stopDate" }] }, "-",
                           { $toString: { $year: "$stopDate" } }
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
                  "_id": "$_id",
                  "empEsaLink": "$empEsaLink",
                  "ctsEmpId": "$ctsEmpId",
                  "startDate": "$startDate",
                  "stopDate": "$stopDate",
                  "halfDay": "$halfDay",
                  "reason": "$reason",
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
                        year: { $toInt: { $substr: ["$stopDate", 4, -1] } },
                        month: { $toInt: { $substr: ["$stopDate", 2, 2] } },
                        day: { $toInt: { $substr: ["$stopDate", 0, 2] } },
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
                  "startDate": "$leaveStart",
                  "stopDate": "$leaveStop",
                  "days": {
                     $cond: {
                        if: { $eq: ["$halfDay", "Y"] }, then: {
                           $divide: [{ $add: [{ $subtract: ["$leaveStop", "$leaveStart"] }, mSecInDay] }, (mSecInDay * 2)]
                        },
                        else: { $divide: [{ $add: [{ $subtract: ["$leaveStop", "$leaveStart"] }, mSecInDay] }, mSecInDay] }
                     }
                  },
                  "halfDay": "$halfDay",
                  "reason": "$reason"
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
                  stopDate: {
                     $let: {
                        vars: {
                           monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                        },
                        in: {
                           $concat: [
                              { $toString: { $dayOfMonth: "$stopDate" } }, "-",
                              { $arrayElemAt: ["$$monthsInString", { $month: "$stopDate" }] }, "-",
                              { $toString: { $year: "$stopDate" } }
                           ]
                        }
                     }
                  }
               }
            }
         ]).toArray((err, selfLeaveArr) => {
            if (err) {
               reject("DB error in " + funcName + ": " + err);
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
function getBuffer(empEsaLink, ctsEmpId, revenueYear, callerName) {
   let funcName = getBuffer.name;
   return new Promise((resolve, reject) => {
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
               "reason": "$reason"
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
            reject("DB error in " + funcName + "function: " + err);
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
function getProjection(recordId, revenueYear) {
   let funcName = getProjection.name;
   return new Promise((resolve, reject) => {
      if (revenueYear === undefined || revenueYear === "") {
         reject(funcName + ": Revenue year is not provided");
      } else if (recordId === undefined || recordId === "") {
         reject(funcName + ": Record id is not provided");
      }

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
               "sowStart": "$sowStart",
               "sowStop": "$sowStop",
               "foreseenSowStop": "$foreseenSowStop",
               "cityCode": "$empEsaLoc.cityCode",
               "cityName": "$empEsaLoc.cityName",
               "wrkHrPerDay": { $toInt: "$wrkHrPerDay" },
               "billRatePerHr": { $toInt: "$billRatePerHr" },
               "currency": "$empEsaProj.currency",
               "empEsaLink": "$empEsaLink"
            }
         },
         {
            $addFields: {
               sowStart: {
                  $cond: {
                     if: { $ne: ["$sowStart", ""] }, then: {
                        $let: {
                           vars: {
                              monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                           },
                           in: {
                              $concat: [
                                 { $toString: { $toInt: { $substr: ["$sowStart", 0, 2] } } }, "-",
                                 { $arrayElemAt: ["$$monthsInString", { $toInt: { $substr: ["$sowStart", 2, 2] } }] }, "-",
                                 { $substr: ["$sowStart", 4, -1] }
                              ]
                           }
                        }
                     }, else: "$sowStart"
                  }
               },
               sowStop: {
                  $cond: {
                     if: { $ne: ["$sowStop", ""] }, then: {
                        $let: {
                           vars: {
                              monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                           },
                           in: {
                              $concat: [
                                 { $toString: { $toInt: { $substr: ["$sowStop", 0, 2] } } }, "-",
                                 { $arrayElemAt: ["$$monthsInString", { $toInt: { $substr: ["$sowStop", 2, 2] } }] }, "-",
                                 { $substr: ["$sowStop", 4, -1] }
                              ]
                           }
                        }
                     }, else: "$sowStop"
                  }
               },
               foreseenSowStop: {
                  $cond: {
                     if: { $ne: ["$foreseenSowStop", ""] }, then: {
                        $let: {
                           vars: {
                              monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                           },
                           in: {
                              $concat: [
                                 { $toString: { $toInt: { $substr: ["$foreseenSowStop", 0, 2] } } }, "-",
                                 { $arrayElemAt: ["$$monthsInString", { $toInt: { $substr: ["$foreseenSowStop", 2, 2] } }] }, "-",
                                 { $substr: ["$foreseenSowStop", 4, -1] }
                              ]
                           }
                        }
                     }, else: "$foreseenSowStop"
                  }
               }
            }
         }
      ]).toArray(async (err, empProjection) => {
         if (err) {
            reject("DB error in " + funcName + " function: " + err);
         } else if (empProjection.length === 1) {
            let empEsaLink = empProjection[0].empEsaLink;
            let strCtsEmpId = `${empProjection[0].ctsEmpId}`;
            let cityCode = empProjection[0].cityCode;

            await getYearlySelfLeaves(empEsaLink, strCtsEmpId, revenueYear, funcName).then((selfLeaveArr) => {
               if (selfLeaveArr.length === 0) {
                  empProjection.push({ "leaves": ["No leaves between " + dateFormat(revenueStart, "d-mmm-yyyy") + " and " + dateFormat(revenueStop, "d-mmm-yyyy")] });
               } else {
                  empProjection.push({ "leaves": selfLeaveArr });
               }
            }).catch((getYearlySelfLeavesErr) => { reject(getYearlySelfLeavesErr); });

            await locObj.getYearlyLocationLeaves(cityCode, revenueYear, funcName).then((locHolArr) => {
               if (locHolArr.length === 0) {
                  empProjection.push({ "publicHolidays": ["No location holidays between " + dateFormat(revenueStart, "d-mmm-yyyy") + " and " + dateFormat(revenueStop, "d-mmm-yyyy")] });
               } else {
                  empProjection.push({ "publicHolidays": locHolArr });
               }
            }).catch((getYearlyLocationLeavesErr) => { reject(getYearlyLocationLeavesErr); });

            await splWrkObj.getSplWrkDays(empEsaLink, strCtsEmpId, cityCode, revenueStart, revenueStop, funcName).then((splWrkArr) => {
               if (splWrkArr.length === 0) {
                  empProjection.push({ "specialWorkDays": ["No additional workdays between " + dateFormat(revenueStart, "d-mmm-yyyy") + " and " + dateFormat(revenueStop, "d-mmm-yyyy")] });
               } else {
                  empProjection.push({ "specialWorkDays": splWrkArr });
               }
            }).catch((getSplWrkDaysErr) => { reject(getSplWrkDaysErr); });

            await getBuffer(empEsaLink, strCtsEmpId, revenueYear, funcName).then((bufferArr) => {
               if (bufferArr.length === 0) {
                  empProjection.push({ "buffers": ["No buffers between " + dateFormat(revenueStart, "d-mmm-yyyy") + " and " + dateFormat(revenueStop, "d-mmm-yyyy")] });
               } else {
                  empProjection.push({ "buffers": bufferArr });
               }
            }).catch((getBufferErr) => { reject(getBufferErr); });

            revObj.computeRevenue(empProjection, revenueYear, funcName).then((revenueArr) => {
               empProjection.push({ "revenue": revenueArr });
               resolve(empProjection);
            }).catch((computeRevenueErr) => { reject(computeRevenueErr); });
         } else if (empProjection.length === 0) {
            reject(funcName + ": No records found")
         } else if (empProjection.length > 1) {
            reject(funcName + ": More than one record found");
         }
      });
   });
}




module.exports = {
   listAssociates,
   getYearlySelfLeaves,
   getSelfLeaveDates,
   getBuffer,
   getProjection
}