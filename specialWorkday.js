const dbObj = require("./database");
const commObj = require("./utility");
const empSplWrkObj = "emp_spl_wrk";
const locSplWrkObj = "loc_spl_wrk";
const leaveHour = 4;
const mSecInDay = 86400000;


function getEmpSplWrk(employeeFilter, splWrkStart, splWrkStop, callerName) {
   let funcName = getEmpSplWrk.name;
   return new Promise((resolve, reject) => {
      if (employeeFilter === undefined || employeeFilter === "") {
         reject(funcName + ": Employee filter not provided");
      } else if (splWrkStart === undefined || splWrkStart === "") {
         reject(callerName + " -> " + funcName + ": Leave start date is not provided");
      } else if (splWrkStop === undefined || splWrkStop === "") {
         reject(callerName + " -> " + funcName + ": Leave stop date is not provided");
      } else {
         let refSplWrkStart = new Date(splWrkStart);
         refSplWrkStart.setUTCHours(0, 0, 0, 0);
         let refSplWrkStop = new Date(splWrkStop);
         refSplWrkStop.setUTCHours(0, 0, 0, 0);

         dbObj.getDb().collection(empSplWrkObj).aggregate([
            {
               $project: {
                  "employeeLinker": "$employeeLinker",
                  "startDate": "$startDate",
                  "stopDate": "$stopDate",
                  "workHour": "$workHour",
                  "reason": "$reason",
                  "workStart": {
                     $dateFromParts: {
                        year: { $toInt: { $substr: ["$startDate", 4, -1] } },
                        month: { $toInt: { $substr: ["$startDate", 2, 2] } },
                        day: { $toInt: { $substr: ["$startDate", 0, 2] } },
                        hour: 0, minute: 0, second: 0, millisecond: 0
                     }
                  },
                  "workStop": {
                     $dateFromParts: {
                        year: { $toInt: { $substr: ["$stopDate", 4, -1] } },
                        month: { $toInt: { $substr: ["$stopDate", 2, 2] } },
                        day: { $toInt: { $substr: ["$stopDate", 0, 2] } },
                        hour: 0, minute: 0, second: 0, millisecond: 0
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
                           { "workStart": { $lte: refSplWrkStart } },
                           { "workStop": { $gte: refSplWrkStop } }
                        ]
                     },
                     {
                        $and: [
                           { "workStart": { $lte: refSplWrkStart } },
                           { "workStop": { $gte: refSplWrkStart } },
                           { "workStop": { $lte: refSplWrkStop } }
                        ]
                     },
                     {
                        $and: [
                           { "workStart": { $gte: refSplWrkStart } },
                           { "workStart": { $lte: refSplWrkStop } },
                           { "workStop": { $gte: refSplWrkStart } },
                           { "workStop": { $lte: refSplWrkStop } }
                        ]
                     },
                     {
                        $and: [
                           { "workStart": { $gte: refSplWrkStart } },
                           { "workStart": { $lte: refSplWrkStop } },
                           { "workStop": { $gte: refSplWrkStart } },
                           { "workStop": { $lte: refSplWrkStop } }
                        ]
                     }
                  ]
               }
            },
            {
               $project: {
                  "_id": "$employeeLinker",
                  "startDate": "$workStart",
                  "stopDate": "$workStop",
                  "workHour": "$workHour",
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
         ]).toArray((err, empSplWrkArr) => {
            if (err) {
               reject("DB error in " + funcName + ": " + err);
            } else {
               resolve(empSplWrkArr);
            }
         });
      }
   });
}


function getLocSplWrk(cityCode, splWrkStart, splWrkStop, callerName) {
   let funcName = getLocSplWrk.name;
   return new Promise((resolve, reject) => {
      if (cityCode === undefined || cityCode === "") {
         reject(callerName + " -> " + funcName + ": City code is not provided");
      } else if (splWrkStart === undefined || splWrkStart === "") {
         reject(callerName + " -> " + funcName + ": Leave start date is not provided");
      } else if (splWrkStop === undefined || splWrkStop === "") {
         reject(callerName + " -> " + funcName + ": Leave stop date is not provided");
      } else {
         let refSplWrkStart = new Date(splWrkStart);
         refSplWrkStart.setUTCHours(0, 0, 0, 0);
         let refSplWrkStop = new Date(splWrkStop);
         refSplWrkStop.setUTCHours(0, 0, 0, 0);
         dbObj.getDb().collection(locSplWrkObj).aggregate([
            {
               $project: {
                  "cityCode": "$cityCode",
                  "startDate": "$startDate",
                  "stopDate": "$stopDate",
                  "halfDay": "$halfDay",
                  "reason": "$reason",
                  "workStart": {
                     $dateFromParts: {
                        year: { $toInt: { $substr: ["$startDate", 4, -1] } },
                        month: { $toInt: { $substr: ["$startDate", 2, 2] } },
                        day: { $toInt: { $substr: ["$startDate", 0, 2] } },
                        hour: 0, minute: 0, second: 0, millisecond: 0
                     }
                  },
                  "workStop": {
                     $dateFromParts: {
                        year: { $toInt: { $substr: ["$stopDate", 4, -1] } },
                        month: { $toInt: { $substr: ["$stopDate", 2, 2] } },
                        day: { $toInt: { $substr: ["$stopDate", 0, 2] } },
                        hour: 0, minute: 0, second: 0, millisecond: 0
                     }
                  }
               }
            },
            {
               $match: {
                  "cityCode": cityCode,
                  $or: [
                     {
                        $and: [
                           { "workStart": { $lte: refSplWrkStart } },
                           { "workStop": { $gte: refSplWrkStop } }
                        ]
                     },
                     {
                        $and: [
                           { "workStart": { $lte: refSplWrkStart } },
                           { "workStop": { $gte: refSplWrkStart } },
                           { "workStop": { $lte: refSplWrkStop } }
                        ]
                     },
                     {
                        $and: [
                           { "workStart": { $gte: refSplWrkStart } },
                           { "workStart": { $lte: refSplWrkStop } },
                           { "workStop": { $gte: refSplWrkStart } },
                           { "workStop": { $lte: refSplWrkStop } }
                        ]
                     },
                     {
                        $and: [
                           { "workStart": { $gte: refSplWrkStart } },
                           { "workStart": { $lte: refSplWrkStop } },
                           { "workStop": { $gte: refSplWrkStart } },
                           { "workStop": { $lte: refSplWrkStop } }
                        ]
                     }
                  ]
               }
            },
            {
               $project: {
                  "_id": "$cityCode",
                  "startDate": "$workStart",
                  "stopDate": "$workStop",
                  "days": {
                     $cond: {
                        if: { $eq: ["$halfDay", "Y"] }, then: {
                           $divide: [{ $add: [{ $subtract: ["$workStop", "$workStart"] }, mSecInDay] }, (mSecInDay * 2)]
                        },
                        else: { $divide: [{ $add: [{ $subtract: ["$workStop", "$workStart"] }, mSecInDay] }, mSecInDay] }
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
         ]).toArray((err, locSplWrkArr) => {
            if (err) {
               reject("DB error in " + getLocSplWrk.name + ": " + err);
            } else {
               resolve(locSplWrkArr);
            }
         });
      }
   });
}


function getSplWrkDays(employeeFilter, cityCode, splWrkStart, splWrkStop, callerName) {
   let funcName = getSplWrkDays.name;
   return new Promise((resolve, reject) => {
      if (employeeFilter === undefined || employeeFilter === "") {
         reject(funcName + ": Employee filter not provided");
      } else if (cityCode === undefined || cityCode === "") {
         reject(callerName + " -> " + funcName + ": Work city code is not provided");
      } else if (splWrkStart === undefined || splWrkStart === "") {
         reject(callerName + " -> " + funcName + ": Start date is not provided");
      } else if (splWrkStop === undefined || splWrkStop === "") {
         reject(callerName + " -> " + funcName + ": Stop date is not provided");
      } else {
         getEmpSplWrk(employeeFilter, splWrkStart, splWrkStop, funcName).then((empSplWrkArr) => {
            getLocSplWrk(cityCode, splWrkStart, splWrkStop, funcName).then((locSplWrkArr) => {
               let splWrkArr = { "empSplWrk": empSplWrkArr, "locSplWrk": locSplWrkArr };
               resolve(splWrkArr);
            }).catch((getLocSplWrkErr) => { reject(getLocSplWrkErr) });
         }).catch((getEmpSplWrkErr) => { reject(getEmpSplWrkErr) });
      }
   });
}


module.exports = {
   getSplWrkDays
}