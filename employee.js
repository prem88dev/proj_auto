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



function listAllAssociates() {
   let funcName = listAllAssociates.name;
   return new Promise((resolve, reject) => {
      dbObj.getDb().collection(empProjColl).aggregate([
         { $sort: { "empLname": 1 } },
         {
            $project: {
               "_id": {
                  $concat: [
                     { $toString: "$esaId" }, "-", { $toString: "$esaSubType" }, "-", { $toString: "$ctsEmpId" }, "-",
                     { $toString: "$wrkHrPerDay" }, "-", { $toString: "$billRatePerHr" }
                  ]
               },
               "empFname": "$empFname",
               "empMname": "$empMname",
               "empLname": "$empLname"
            }
         }
      ]).toArray((err, employeeList) => {
         if (err) {
            reject(err);
         } else {
            resolve(employeeList);
         }
      });
   });
}


/*
   listAssociates: to get list of employees in a project

   params:
      esaId - ESA project id for which employees are to be listed
      revenueYear - Year in which the employee's SO is active
   
   returns array of employee with their first, middle and last names
*/
function listAssociates(esaId, revenueYear, callerName) {
   let funcName = listAssociates.name;
   return new Promise((resolve, reject) => {
      if (esaId === undefined || esaId === "") {
         reject(funcName + ": ESA id is not provided");
      } else if (revenueYear === undefined && revenueYear === "") {
         reject(funcName + ": Revenue year is not provided");
      } else {
         let iEsaId = parseInt(esaId, 10);
         let iRevenueYear = parseInt(revenueYear, 10);
         let revenueStart = new Date(iRevenueYear, 0, 2);
         revenueStart.setUTCHours(0, 0, 0, 0);
         let revenueStop = new Date(iRevenueYear, 12, 1);
         revenueStop.setUTCHours(0, 0, 0, 0);
         dbObj.getDb().collection(empProjColl).aggregate([
            {
               $project: {
                  "empFname": "$empFname",
                  "empMname": "$empMname",
                  "empLname": "$empLname",
                  "esaId": "$esaId",
                  "esaSubType": "$esaSubType",
                  "ctsEmpId": "$ctsEmpId",
                  "wrkHrPerDay": "$wrkHrPerDay",
                  "billRatePerHr": "$billRatePerHr",
                  "sowBegin": {
                     $dateFromParts: {
                        year: { $toInt: { $substr: ["$sowStart", 4, -1] } },
                        month: { $toInt: { $substr: ["$sowStart", 2, 2] } },
                        day: { $toInt: { $substr: ["$sowStart", 0, 2] } },
                        hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
                     }
                  },
                  "sowEnd": {
                     $dateFromParts: {
                        year: { $toInt: { $substr: ["$sowStop", 4, -1] } },
                        month: { $toInt: { $substr: ["$sowStop", 2, 2] } },
                        day: { $toInt: { $substr: ["$sowStop", 0, 2] } },
                        hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
                     }
                  },
                  "foreseenSowEnd": {
                     $cond: {
                        if: { $ne: ["$foreseenSowStop", ""] }, then: {
                           $dateFromParts: {
                              year: { $toInt: { $substr: ["$foreseenSowStop", 4, -1] } },
                              month: { $toInt: { $substr: ["$foreseenSowStop", 2, 2] } },
                              day: { $toInt: { $substr: ["$foreseenSowStop", 0, 2] } },
                              hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
                           }
                        }, else: "$foreseenSowStop"
                     }
                  }
               }
            },
            {
               $match: {
                  "esaId": iEsaId,
                  $or: [
                     {
                        $and: [
                           { "sowBegin": { "$lte": revenueStart } },
                           {
                              $or: [
                                 { "sowEnd": { "$gte": revenueStop } },
                                 { "foreseenSowEnd": { "$gte": revenueStop } }
                              ]
                           }
                        ]
                     },
                     {
                        $and: [
                           { "sowBegin": { "$lte": revenueStart } },
                           {
                              $or: [
                                 {
                                    $and: [
                                       { "sowEnd": { "$gte": revenueStart } },
                                       { "sowEnd": { "$lte": revenueStop } }
                                    ]
                                 },
                                 {
                                    $and: [
                                       { "foreseenSowEnd": { "$gte": revenueStart } },
                                       { "foreseenSowEnd": { "$lte": revenueStop } }
                                    ]
                                 }
                              ]
                           }
                        ]
                     },
                     {
                        $and: [
                           { "sowBegin": { "$gte": revenueStart } },
                           { "sowBegin": { "$lte": revenueStop } },
                           {
                              $or: [
                                 {
                                    $and: [
                                       { "sowEnd": { "$gte": revenueStart } },
                                       { "sowEnd": { "$lte": revenueStop } }
                                    ]
                                 },
                                 {
                                    $and: [
                                       { "foreseenSowEnd": { "$gte": revenueStart } },
                                       { "foreseenSowEnd": { "$lte": revenueStop } }
                                    ]
                                 }
                              ]
                           }
                        ]
                     },
                     {
                        $and: [
                           { "sowBegin": { "$gte": revenueStart } },
                           { "sowBegin": { "$lte": revenueStop } },
                           {
                              $or: [
                                 {
                                    $and: [
                                       { "sowEnd": { "$gte": revenueStart } },
                                       { "sowEnd": { "$gte": revenueStop } }
                                    ]
                                 },
                                 {
                                    $and: [
                                       { "sowEnd": { "$gte": revenueStart } },
                                       { "sowEnd": { "$gte": revenueStop } }
                                    ]
                                 }
                              ]
                           }
                        ]
                     }
                  ]
               }
            },
            {
               $project: {
                  "_id": {
                     $concat: [
                        { $toString: "$esaId" }, "-", { $toString: "$esaSubType" }, "-", { $toString: "$ctsEmpId" }, "-",
                        { $toString: "$wrkHrPerDay" }, "-", { $toString: "$billRatePerHr" }
                     ]
                  },
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


function getEmployeeLeaves(employeeFilter, leaveStartDate, leaveStopDate, callerName) {
   let funcName = getEmployeeLeaves.name;
   return new Promise((resolve, reject) => {
      if (employeeFilter === undefined || employeeFilter === "") {
         reject(funcName + ": Employee filter is not provided");
      } else if (leaveStartDate === undefined || leaveStartDate === "") {
         reject(funcName + ": Personal leave start date is not provided");
      } else if (leaveStopDate === undefined || leaveStopDate === "") {
         reject(funcName + ": Personal leave stop date is not provided");
      } else {
         let refStartDate = new Date(leaveStartDate);
         refStartDate.setUTCHours(0, 0, 0, 0);
         let refStopDate = new Date(leaveStopDate);
         refStopDate.setUTCHours(0, 0, 0, 0);

         dbObj.getDb().collection(empLeaveColl).aggregate([
            {
               $project: {
                  "employeeLinker": "$employeeLinker",
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
                  "employeeLinker": employeeFilter,
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
                  "_id": "$employeeLinker",
                  "startDate": "$leaveStart",
                  "stopDate": "$leaveStop",
                  "days": {
                     $cond: {
                        if: { $eq: ["$halfDay", "Y"] }, then: {
                           $divide: [{ $add: [{ $subtract: ["$leaveStop", "$leaveStart"] }, mSecInDay] }, (mSecInDay * 2)]
                        },
                        else: {
                           $divide: [{ $add: [{ $subtract: ["$leaveStop", "$leaveStart"] }, mSecInDay] }, mSecInDay]
                        }
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
function getBuffer(employeeFilter, bufferStartDate, bufferStopDate, callerName) {
   let funcName = getBuffer.name;
   return new Promise((resolve, reject) => {
      if (employeeFilter === undefined || employeeFilter === "") {
         reject(funcName + ": Employee filter is not provided");
      } else if (bufferStartDate === undefined || bufferStartDate === "") {
         reject(funcName + ": Buffer start date is not provided");
      } else if (bufferStopDate === undefined || bufferStopDate === "") {
         reject(funcName + ": Buffer stop date is not provided");
      } else {
         let refStartDate = new Date(bufferStartDate);
         refStartDate.setUTCHours(0, 0, 0, 0);
         let refStopDate = new Date(bufferStopDate);
         refStopDate.setUTCHours(0, 0, 0, 0);

         dbObj.getDb().collection(empBuffer).aggregate([
            {
               $project: {
                  "employeeLinker": "$employeeLinker",
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
                  "employeeLinker": employeeFilter,
                  $and: [
                     { "bufferDate": { "$gte": refStartDate } },
                     { "bufferDate": { "$lte": refStopDate } }
                  ]
               }
            },
            {
               $project: {
                  "_id": "$employeeLinker",
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
function getProjection(revenueYear, employeeFilter, callerName) {
   let funcName = getProjection.name;
   return new Promise((resolve, reject) => {
      if (revenueYear === undefined || revenueYear === "" || revenueYear.length < 4) {
         reject(funcName + ": Revenue year not provided");
      } else if (employeeFilter === undefined || employeeFilter === "") {
         reject(funcName + ": Employee filter not provided");
      } else {
         let iRevenueYear = parseInt(revenueYear, 10);
         let revenueStart = new Date(iRevenueYear, 0, 2);
         revenueStart.setUTCHours(0, 0, 0, 0);
         let revenueStop = new Date(iRevenueYear, 12, 1);
         revenueStop.setUTCHours(0, 0, 0, 0);
         let iEsaId = 0;
         let iEsaSubType = -1; /* sub-type of esa id has zero-based index */
         let iCtsEmpId = 0;
         let iWrkHrPerDay = 0;
         let iBillingRatePerHr = 0;
         let filterSplit = employeeFilter.split("-");
         if (filterSplit.length === 5) {
            filterSplit.forEach((splitVal, idx) => {
               if (idx === 0) {
                  iEsaId = parseInt(splitVal, 10);
               } else if (idx === 1) {
                  iEsaSubType = parseInt(splitVal, 10);
               } else if (idx === 2) {
                  iCtsEmpId = parseInt(splitVal, 10);
               } else if (idx === 3) {
                  iWrkHrPerDay = parseInt(splitVal, 10);
               } else if (idx === 4) {
                  iBillingRatePerHr = parseInt(splitVal, 10);
               }
            });

            if (iEsaId === 0 || iEsaSubType === -1 || iCtsEmpId === 0 || iWrkHrPerDay === 0 || iBillingRatePerHr === 0) {
               reject(funcName + ": Filter parameter is not proper");
            } else {
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
                        "esaId": iEsaId,
                        "esaSubType": iEsaSubType,
                        "ctsEmpId": iCtsEmpId,
                        "wrkHrPerDay": iWrkHrPerDay,
                        "billRatePerHr": iBillingRatePerHr
                     }
                  },
                  {
                     $project: {
                        "_id": {
                           $concat: [
                              { $toString: "$esaId" }, "-", { $toString: "$esaSubType" }, "-", { $toString: "$ctsEmpId" }, "-",
                              { $toString: "$wrkHrPerDay" }, "-", { $toString: "$billRatePerHr" }
                           ]
                        },
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
               ]).toArray((err, empProjection) => {
                  if (err) {
                     reject("DB error in " + funcName + ": " + err);
                  } else if (empProjection.length === 1) {
                     let employeeFilter = empProjection[0]._id;
                     let cityCode = empProjection[0].cityCode;

                     getEmployeeLeaves(employeeFilter, revenueStart, revenueStop, funcName).then((selfLeaveArr) => {
                        empProjection.push({ "leaves": selfLeaveArr });
                        locObj.getPublicHolidays(cityCode, revenueStart, revenueStop, funcName).then((locHolArr) => {
                           empProjection.push({ "publicHolidays": locHolArr });
                           splWrkObj.getSplWrkDays(employeeFilter, cityCode, revenueStart, revenueStop, funcName).then((splWrkArr) => {
                              empProjection.push({ "specialWorkDays": splWrkArr });
                              getBuffer(employeeFilter, revenueStart, revenueStop, funcName).then((bufferArr) => {
                                 empProjection.push({ "buffers": bufferArr });
                                 revObj.computeRevenue(empProjection, iRevenueYear, funcName).then((revenueArr) => {
                                    empProjection.push({ "revenue": revenueArr });
                                    resolve(empProjection);
                                 }).catch((computeRevenueErr) => { reject(computeRevenueErr); });
                              }).catch((getBufferErr) => { reject(getBufferErr); });
                           }).catch((getSplWrkDaysErr) => { reject(getSplWrkDaysErr); });
                        }).catch((getPublicHolidaysErr) => { reject(getPublicHolidaysErr); });
                     }).catch((getEmployeeLeavesErr) => { reject(getEmployeeLeavesErr); });
                  } else if (empProjection.length === 0) {
                     reject(funcName + ": No records found for filter [" + filterParam + "]");
                  } else if (empProjection.length > 1) {
                     reject(funcName + ": More than one record found for filter [" + filterParam + "]");
                  }
               });
            }
         } else {
            reject(funcName + ": Expected filter parameter is not provided");
         }
      }
   });
}

/* get min and max allocation year for manufacturing year drop down */
function getMinMaxAllocationYear(esaId, caller) {
   let funcName = getMinMaxAllocationYear.name;
   return new Promise((resolve, reject) => {
      if (esaId === undefined || esaId === "") {
         reject(funcName + ": ESA ID not provided");
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
                  "esaId": "$esaId",
                  "sowBegin": {
                     $dateFromParts: {
                        year: { $toInt: { $substr: ["$sowStart", 4, -1] } },
                        month: { $toInt: { $substr: ["$sowStart", 2, 2] } },
                        day: { $toInt: { $substr: ["$sowStart", 0, 2] } },
                        hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
                     }
                  },
                  "sowEnd": {
                     $dateFromParts: {
                        year: { $toInt: { $substr: ["$sowStop", 4, -1] } },
                        month: { $toInt: { $substr: ["$sowStop", 2, 2] } },
                        day: { $toInt: { $substr: ["$sowStop", 0, 2] } },
                        hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
                     }
                  },
                  "foreseenSowEnd": {
                     $cond: {
                        if: { $ne: ["$foreseenSowStop", ""] }, then: {
                           $dateFromParts: {
                              year: { $toInt: { $substr: ["$foreseenSowStop", 4, -1] } },
                              month: { $toInt: { $substr: ["$foreseenSowStop", 2, 2] } },
                              day: { $toInt: { $substr: ["$foreseenSowStop", 0, 2] } },
                              hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
                           }
                        }, else: {
                           $dateFromParts: {
                              year: { $toInt: { $substr: ["$sowStop", 4, -1] } },
                              month: { $toInt: { $substr: ["$sowStop", 2, 2] } },
                              day: { $toInt: { $substr: ["$sowStop", 0, 2] } },
                              hour: 0, minute: 0, second: 0, millisecond: 0, timezone: "UTC"
                           }
                        }
                     }
                  }
               }
            },
            {
               $group: {
                  "_id": "$esaId",
                  "minYear": { $min: { $year: "$sowBegin" } },
                  "maxYear": {
                     $max: {
                        $cond: {
                           if: { $gt: ["$foreseenSowStop", "$sowEnd"] }, then: {
                              $year: "$foreseenSowStop"
                           }, else: {
                              $year: "$sowEnd"
                           }
                        }
                     }
                  }
               }
            }
         ]).toArray((err, minMaxYear) => {
            if (err) {
               reject("DB error in " + funcName + ": " + err);
            } else {
               resolve(minMaxYear);
            }
         });
      }
   })
}


module.exports = {
   listAssociates,
   listAllAssociates,
   getEmployeeLeaves,
   getBuffer,
   getProjection,
   getMinMaxAllocationYear
}