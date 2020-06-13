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
const esaProjColl = "esa_proj"
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
   listAssociates: to get list of employees in a project

   params:
      esaId - ESA project id for which employees are to be listed
   
   returns array of employee with their first, middle and last names
*/
function listAssociates(esaId) {
   let funcName = listAssociates.name;
   return new Promise((resolve, reject) => {
      if (esaId === undefined || esaId === "") {
         reject(funcName + ": ESA id is not provided");
      } else {
         let iEsaId = parseInt(esaId, 10);
         dbObj.getDb().collection(empProjColl).aggregate([
            {
               $match: {
                  "esaId": iEsaId
               }
            },
            {
               $project: {
                  "_id": "$_id",
                  "empFname": "$empFname",
                  "empMname": "$empMname",
                  "empLname": "$empLname"
               }
            }
         ]).toArray(function (err, allProj) {
            if (err) {
               reject("DB error in " + funcName + ": " + err);
            } else {
               resolve(allProj);
            }
         });
      }
   });
}


function getYearlySelfLeaves(esaId, esaSubType, ctsEmpId, revenueYear) {
   let funcName = getYearlySelfLeaves.name;
   return new Promise((resolve, reject) => {
      if (esaId === undefined || esaId === "") {
         reject(funcName + ": ESA ID is not provided");
      } else if (esaSubType === undefined || esaSubType === "") {
         reject(funcName + ": ESA sub type is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(funcName + ": Employee ID is not provided");
      } else if (revenueYear === undefined || revenueYear === "") {
         reject(funcName + ": Revenue year is not provided");
      } else {
         let iEsaId = parseInt(esaId, 10);
         let iEsaSubType = parseInt(esaSubType, 10);
         let iCtsEmpId = parseInt(ctsEmpId, 10);
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
                  "esaSubType": "$esaSubType",
                  "ctsEmpId": "$ctsEmpId",
                  "startDate": "$startDate",
                  "stopDate": "$stopDate",
                  "halfDay": "$halfDay",
                  "leaveHour": "$leaveHour",
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
                  "esaId": iEsaId,
                  "esaSubType": iEsaSubType,
                  "ctsEmpId": iCtsEmpId,
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
                  "leaveHour": "$leaveHour",
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
      }
   });
}


function getSelfLeaveDates(esaId, esaSubType, ctsEmpId, selfLeaveStart, selfLeaveStop, callerName) {
   let funcName = getSelfLeaveDates.name;
   return new Promise((resolve, reject) => {
      if (esaId === undefined || esaId === "") {
         reject(funcName + ": ESA ID is not provided");
      } else if (esaSubType === undefined || esaSubType === "") {
         reject(funcName + ": ESA sub type is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(funcName + ": Employee ID is not provided");
      } else if (selfLeaveStart === undefined || selfLeaveStart === "") {
         reject(funcName + ": Personal leave start date is not provided");
      } else if (selfLeaveStop === undefined || selfLeaveStop === "") {
         reject(funcName + ": Personal leave stop date is not provided");
      } else {
         let iEsaId = parseInt(esaId, 10);
         let iEsaSubType = parseInt(esaSubType, 10);
         let iCtsEmpId = parseInt(ctsEmpId, 10);
         let refStartDate = new Date(selfLeaveStart);
         refStartDate.setUTCHours(0, 0, 0, 0);
         let refStopDate = new Date(selfLeaveStop);
         refStopDate.setUTCHours(0, 0, 0, 0);
         dbObj.getDb().collection(empLeaveColl).aggregate([
            {
               $project: {
                  "_id": "$_id",
                  "esaId": "$esaId",
                  "esaSubType": "$esaSubType",
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
                  "esaId": iEsaId,
                  "esaSubType": iEsaSubType,
                  "ctsEmpId": iCtsEmpId,
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
         esaId - ESA project id
         easSubType - ESA project's sub type
         ctsEmpId - employee id
         revenueYear - year for which leaves are required
*/
function getBuffer(esaId, esaSubType, ctsEmpId, revenueYear, callerName) {
   let funcName = getBuffer.name;
   return new Promise((resolve, reject) => {
      if (esaId === undefined || esaId === "") {
         reject(funcName + ": ESA ID is not provided");
      } else if (esaSubType === undefined || esaSubType === "") {
         reject(funcName + ": ESA sub type is not provided");
      } else if (ctsEmpId === undefined || ctsEmpId === "") {
         reject(funcName + ": Employee ID is not provided");
      } else if (revenueYear === undefined || revenueYear === "") {
         reject(funcName + ": Revenue year is not provided");
      } else {
         let iEsaId = parseInt(esaId, 10);
         let iEsaSubType = parseInt(esaSubType, 10);
         let iCtsEmpId = parseInt(ctsEmpId, 10);
         let calcYear = parseInt(revenueYear, 10);
         let revenueStart = new Date(calcYear, 0, 2);
         revenueStart.setUTCHours(0, 0, 0, 0);
         let revenueStop = new Date(calcYear, 12, 1);
         revenueStop.setUTCHours(0, 0, 0, 0);
         dbObj.getDb().collection(empBuffer).aggregate([
            {
               $project: {
                  "_id": "$_id",
                  "esaId": "$esaId",
                  "esaSubType": "$esaSubType",
                  "ctsEmpId": "$ctsEmpId",
                  "month": "$month",
                  "hours": "$hours",
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
                  "esaId": iEsaId,
                  "esaSubType": iEsaSubType,
                  "ctsEmpId": iCtsEmpId,
                  $and: [
                     { "bufferDate": { "$gte": revenueStart } },
                     { "bufferDate": { "$lte": revenueStop } }
                  ]
               }
            },
            {
               $project: {
                  "_id": "$_id",
                  "month": "$month",
                  "hours": "$hours",
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
               reject("DB error in " + funcName + ": " + err);
            } else {
               resolve(bufferArr);
            }
         });
      }
   });
}


/* get projection data for specific employee in selected project */
function getProjection(empRecId, revenueYear, callerName) {
   let funcName = getProjection.name;
   return new Promise((resolve, reject) => {
      if (revenueYear === undefined || revenueYear === "") {
         reject(funcName + ": Revenue year is not provided");
      } else if (empRecId === undefined || empRecId === "") {
         reject(funcName + ": Record id is not provided");
      } else {
         let empRecObjId = new ObjectId(empRecId);
         let intRevenueYear = parseInt(revenueYear, 10);
         let revenueStart = new Date(intRevenueYear, 0, 2);
         revenueStart.setUTCHours(0, 0, 0, 0);
         let revenueStop = new Date(intRevenueYear, 12, 1);
         revenueStop.setUTCHours(0, 0, 0, 0);
         dbObj.getDb().collection(empProjColl).aggregate([
            {
               $lookup: {
                  from: "esa_proj",
                  let: {
                     esaProjId: "$esaId",
                     esaProjSubType: "$esaSubType"
                  },
                  pipeline: [{
                     $match: {
                        $expr: {
                           $and: [
                              { $eq: ["$esaId", "$$esaProjId"] },
                              { $eq: ["$esaSubType", "$$esaProjSubType"] }
                           ]
                        }
                     }
                  }],
                  as: "esa_proj_match"
               }
            },
            {
               $unwind: "$esa_proj_match"
            },
            {
               $lookup: {
                  from: "wrk_loc",
                  localField: "cityCode",
                  foreignField: "cityCode",
                  as: "wrk_loc_match"
               }
            },
            {
               $unwind: "$wrk_loc_match"
            },
            {
               $match: {
                  "_id": empRecObjId
               }
            },
            {
               $project: {
                  "_id": "$_id",
                  "esaId": "$esaId",
                  "esaSubType": "$esaSubType",
                  "esaDesc": "$esa_proj_match.esaDesc",
                  "projName": "$projName",
                  "ctsEmpId": "$ctsEmpId",
                  "empFname": "$empFname",
                  "empMname": "$empMname",
                  "empLname": "$empLname",
                  "lowesUid": "$lowesUid",
                  "deptName": "$deptName",
                  "sowStart": "$sowStart",
                  "sowStop": "$sowStop",
                  "foreseenSowStop": "$foreseenSowStop",
                  "cityCode": "$wrk_loc_match.cityCode",
                  "cityName": "$wrk_loc_match.cityName",
                  "siteInd": "$wrk_loc_match.siteInd",
                  "wrkHrPerDay": "$wrkHrPerDay",
                  "billRatePerHr": "$billRatePerHr",
                  "currency": "$esa_proj_match.currency"
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
               reject("DB error in " + funcName + ": " + err);
            } else if (empProjection.length === 1) {
               let esaId = empProjection[0].esaId;
               let esaSubType = empProjection[0].esaSubType
               let ctsEmpId = empProjection[0].ctsEmpId;
               let cityCode = empProjection[0].cityCode;

               getYearlySelfLeaves(empRecId, revenueYear, funcName)
                  .then((selfLeaveArr) => {
                  if (selfLeaveArr.length === 0) {
                     empProjection.push({ "leaves": ["No leaves between " + dateFormat(revenueStart, "d-mmm-yyyy") + " and " + dateFormat(revenueStop, "d-mmm-yyyy")] });
                  } else {
                     empProjection.push({ "leaves": selfLeaveArr });
                  }
                  })
                  .then(() => {
                     locObj.getYearlyLocationLeaves(cityCode, revenueYear, funcName)
                        .then((locHolArr) => {
                     if (locHolArr.length === 0) {
                        empProjection.push({ "publicHolidays": ["No location holidays between " + dateFormat(revenueStart, "d-mmm-yyyy") + " and " + dateFormat(revenueStop, "d-mmm-yyyy")] });
                     } else {
                        empProjection.push({ "publicHolidays": locHolArr });
                     }
                        })
                        .catch((getYearlyLocationLeavesErr) => { reject(getYearlyLocationLeavesErr); });
                  })
                  .then(() => {
                     splWrkObj.getSplWrkDays(empRecId, cityCode, revenueStart, revenueStop, funcName)
                        .then((splWrkArr) => {
                        if (splWrkArr.length === 0) {
                           empProjection.push({ "specialWorkDays": ["No additional workdays between " + dateFormat(revenueStart, "d-mmm-yyyy") + " and " + dateFormat(revenueStop, "d-mmm-yyyy")] });
                        } else {
                           empProjection.push({ "specialWorkDays": splWrkArr });
                        }
                        })
                        .catch((getSplWrkDaysErr) => { reject(getSplWrkDaysErr); });
                  })
                  .then(() => {
                     getBuffer(empRecId, revenueYear, funcName)
                        .then((bufferArr) => {
                           if (bufferArr.length === 0) {
                              empProjection.push({ "buffers": ["No buffers between " + dateFormat(revenueStart, "d-mmm-yyyy") + " and " + dateFormat(revenueStop, "d-mmm-yyyy")] });
                           } else {
                              empProjection.push({ "buffers": bufferArr });
                           }
                        })
                        .catch((getBufferErr) => { reject(getBufferErr); });
                  })
                  .then(() => {
                     revObj.computeRevenue(empProjection, revenueYear, funcName)
                        .then((revenueArr) => {
                              empProjection.push({ "revenue": revenueArr });
                              resolve(empProjection);
                        })
                        .catch((computeRevenueErr) => { reject(computeRevenueErr); });
                  })
                  .catch((getYearlySelfLeavesErr) => { reject(getYearlySelfLeavesErr) });
            } else if (empProjection.length === 0) {
               reject(funcName + ": No records found for object " + empRecId)
            } else if (empProjection.length > 1) {
               reject(funcName + ": More than one record found for object " + empRecId);
            }
         });
      }
   });
}




module.exports = {
   listAssociates,
   getYearlySelfLeaves,
   getSelfLeaveDates,
   getBuffer,
   getProjection
}