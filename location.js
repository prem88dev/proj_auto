const dbObj = require('./database');
const commObj = require('./utility');
const locLeaveColl = "loc_holiday";
const mSecInDay = 86400000;


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
         revenueStop.setUTCHours(0, 0, 0, 0);
         dbObj.getDb().collection(locLeaveColl).aggregate([
            {
               $project: {
                  "_id": "$_id",
                  "cityCode": "$cityCode",
                  "startDate": "$startDate",
                  "stopDate": "$stopDate",
                  "halfDay": "$halfDay",
                  "description": "$description",
                  "leaveStart": {
                     $dateFromString: {
                        dateString: "$startDate",
                        format: "%d%m%Y"
                     }
                  },
                  "leaveStop": {
                     $dateFromString: {
                        dateString: "$stopDate",
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
                           { "leaveStart": { $lte: revenueStart } },
                           { "leaveStop": { $gte: revenueStop } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { $lte: revenueStart } },
                           { "leaveStop": { $gte: revenueStart } },
                           { "leaveStop": { $lte: revenueStop } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { $gte: revenueStart } },
                           { "leaveStart": { $lte: revenueStop } },
                           { "leaveStop": { $gte: revenueStart } },
                           { "leaveStop": { $lte: revenueStop } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { $gte: revenueStart } },
                           { "leaveStart": { $lte: revenueStop } },
                           { "leaveStop": { $gte: revenueStart } },
                           { "leaveStop": { $lte: revenueStop } }
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
                  "description": "$description"
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
                  stopDate: {
                     $let: {
                        vars: {
                           monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                        },
                        in: {
                           $concat: [{ $toString: { $dayOfMonth: "$stopDate" } }, "-",
                           { $arrayElemAt: ["$$monthsInString", { $month: "$stopDate" }] }, "-",
                           { $toString: { $year: "$stopDate" } }]
                        }
                     }
                  }
               }
            }
         ]).toArray((err, locLeaveArr) => {
            if (err) {
               reject("DB error in " + funcName + " function: " + err);
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
                  "_id": "$_id",
                  "cityCode": "$cityCode",
                  "startDate": "$startDate",
                  "stopDate": "$stopDate",
                  "halfDay": "$halfDay",
                  "description": "$description",
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
                  "description": "$description"
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
                  stopDate: {
                     $let: {
                        vars: {
                           monthsInString: [, "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                        },
                        in: {
                           $concat: [{ $toString: { $dayOfMonth: "$stopDate" } }, "-",
                           { $arrayElemAt: ["$$monthsInString", { $month: "$stopDate" }] }, "-",
                           { $toString: { $year: "$stopDate" } }]
                        }
                     }
                  }
               }
            }
         ]).toArray((err, locHolArr) => {
            if (err) {
               reject("DB error in " + funcName + ": " + err);
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