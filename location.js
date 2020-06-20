const dbObj = require('./database');
const commObj = require('./utility');
const locLeaveColl = "loc_holiday";
const mSecInDay = 86400000;


function getPublicHolidays(cityCode, leaveStartDate, leaveStopDate, callerName) {
   let funcName = getPublicHolidays.name;
   return new Promise((resolve, reject) => {
      if (cityCode === undefined || cityCode === "") {
         reject(funcName + ": City code is not provided");
      } else if (leaveStartDate === undefined || leaveStartDate === "") {
         reject(funcName + ": Location holiday start date is not provided");
      } else if (leaveStopDate === undefined || leaveStopDate === "") {
         reject(funcName + ": Location holiday stop date is not provided");
      } else {
         let refStartDate = new Date(leaveStartDate);
         refStartDate.setUTCHours(0, 0, 0, 0);
         let refStopDate = new Date(leaveStopDate);
         refStopDate.setUTCHours(0, 0, 0, 0);
         dbObj.getDb().collection(locLeaveColl).aggregate([
            {
               $project: {
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
               $project: {
                  "_id": "$cityCode",
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
         ]).toArray((err, publicHolidays) => {
            if (err) {
               reject("DB error in " + funcName + ": " + err);
            } else {
               resolve(publicHolidays);
            }
         });
      }
   });
}


module.exports = {
   getPublicHolidays
}