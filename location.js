const dbObj = require('./database');
const commObj = require('./utility');
const locLeaveColl = "loc_holiday";


function getYearlyLocationLeaves(cityCode, revenueYear) {
   let funcName = getYearlyLocationLeaves.name;
   return new Promise((resolve, reject) => {
      if (cityCode === undefined || cityCode === "") {
         reject(getLocationLeave.name + ": Location is not provided");
      } else if (revenueYear === undefined || revenueYear === "") {
         reject(getLocationLeave.name + ": Revenue year is not provided");
      } else {
         let calcYear = parseInt(revenueYear, 10);
         let revenueStart = new Date(calcYear, 0, 2);
         revenueStart.setUTCHours(0, 0, 0, 0);
         let revenueStop = new Date(calcYear, 12, 1);
         revenueStop.setUTCHours(23, 59, 59, 0);
         dbObj.getDb().collection(locLeaveColl).aggregate([
            {
               $project: {
                  "_id": 1,
                  "cityCode": 2,
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
                  "cityCode": cityCode,
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
                     }
                  ]
               }
            },
            {
               $project: {
                  "_id": "$_id",
                  "startDate": "$leaveStart",
                  "endDate": "$leaveEnd",
                  "days": { $toInt: "$days" },
                  "description": "$description",
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
                           $concat: [{ $toString: { $dayOfMonth: "$startDate" } }, "-",
                           { $arrayElemAt: ["$$monthsInString", { $month: "$startDate" }] }, "-",
                           { $toString: { $year: "$startDate" } }]
                        }
                     }
                  },
                  endDate: {
                     $let: {
                        vars: {
                           monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                        },
                        in: {
                           $concat: [{ $toString: { $dayOfMonth: "$endDate" } }, "-",
                           { $arrayElemAt: ["$$monthsInString", { $month: "$startDate" }] }, "-",
                           { $toString: { $year: "$endDate" } }]
                        }
                     }
                  }
               }
            }
         ]).toArray((err, locLeaveArr) => {
            if (err) {
               reject("DB error in " + getLocationLeave.name + " function: " + err);
            } else if (locLeaveArr.length >= 1) {
               commObj.countAllDays(locLeaveArr, funcName).then((allDaysInLeave) => {
                  commObj.countWeekdays(locLeaveArr, funcName).then((workDaysInLeave) => {
                     locLeaveArr.push({ "totalDays": allDaysInLeave, "workDays": workDaysInLeave });
                     resolve(locLeaveArr);
                  });
               });
            } else {
               resolve(locLeaveArr);
            }
         });
      }
   });
}



function getLocHolDates(cityCode, locHolStart, locHolStop, callerName) {
   return new Promise((resolve, reject) => {
      if (cityCode === undefined || cityCode === "") {
         reject(countLocationHolidays.name + ": City code is not provided");
      } else if (locHolStart === undefined || locHolStart === "") {
         reject(countLocationHolidays.name + ": Location holiday start date is not provided");
      } else if (locHolStop === undefined || locHolStop === "") {
         reject(countLocationHolidays.name + ": Location holiday stop date is not provided");
      } else {
         let refStartDate = new Date(locHolStart);
         refStartDate.setUTCHours(0, 0, 0, 0);
         let refStopDate = new Date(locHolStop);
         refStopDate.setUTCHours(0, 0, 0, 0);
         dbObj.getDb().collection(locLeaveColl).aggregate([
            {
               $project: {
                  "_id": 1,
                  "cityCode": 2,
                  "startDate": 3,
                  "endDate": 4,
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
                  "cityCode": cityCode,
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
               $group: {
                  "_id": "$_id",
                  "funcName": getLocHolDates.name,
                  "callerName": callerName,
                  "locHolStart": "$leaveStart",
                  "locHolStop": "$leaveEnd",
                  "refStartDate": refStartDate,
                  "refStopDate": refStopDate
               }
            }
         ]).toArray((err, locHolArr) => {
            if (err) {
               reject("DB error in " + getLocHolDates.name + ": " + err);
            } else {
               resolve(locHolArr);
            }
         });
      }
   });
}


module.exports = {
   getYearlyLocationLeaves,
   getLocHolDates
}