const dbObj = require('./database');
const commObj = require('./utility');
const locLeaveColl = "loc_holiday";
const mSecInDay = 86400000;


function countLocationHolidays(cityCode, fromDate, toDate) {
   return new Promise((resolve, reject) => {
      if (cityCode === undefined || cityCode === "") {
         reject(countLocationHolidays.name + ": City code is not provided");
      } else if (fromDate === undefined || fromDate === "") {
         reject(countLocationHolidays.name + ": Leave start date is not provided");
      } else if (toDate === undefined || toDate === "") {
         reject(countLocationHolidays.name + ": Leave end date is not provided");
      } else {
         let startDate = new Date(fromDate);
         startDate.setUTCHours(0, 0, 0, 0);
         let stopDate = new Date(toDate);
         stopDate.setUTCHours(0, 0, 0, 0);
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
                  "cityCode": cityCode,
                  $or: [
                     {
                        $and: [
                           { "leaveStart": { "$lte": startDate } },
                           { "leaveEnd": { "$gte": stopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$lte": startDate } },
                           { "leaveEnd": { "$gte": startDate } },
                           { "leaveEnd": { "$lte": stopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$gte": startDate } },
                           { "leaveStart": { "$lte": stopDate } },
                           { "leaveEnd": { "$gte": startDate } },
                           { "leaveEnd": { "$lte": stopDate } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$gte": startDate } },
                           { "leaveStart": { "$lte": stopDate } },
                           { "leaveEnd": { "$gte": startDate } },
                           { "leaveEnd": { "$gte": stopDate } }
                        ]
                     }
                  ]
               }
            },
            {
               $project: {
                  "_id": "$_id",
                  "cityCode": "$cityCode",
                  "startDate": "$leaveStart",
                  "endDate": "$leaveEnd",
                  "calcDays": {
                     $switch: {
                        branches: [
                           { case: { $eq: ["$leaveStart", "$leaveEnd"] }, then: 1 },
                           {
                              case: {
                                 $and: [
                                    { $lte: ["$leaveStart", startDate] },
                                    { $gte: ["$leaveEnd", stopDate] }
                                 ]
                              }, then: {
                                 $divide: [{ $subtract: [stopDate, startDate] }, mSecInDay]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $lte: ["$leaveStart", startDate] },
                                    { $gte: ["$leaveEnd", startDate] },
                                    { $lte: ["$leaveEnd", stopDate] }
                                 ]
                              }, then: {
                                 $divide: [{ $subtract: ["$leaveEnd", startDate] }, mSecInDay]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $gte: ["$leaveStart", startDate] },
                                    { $lte: ["$leaveStart", stopDate] },
                                    { $gte: ["$leaveEnd", startDate] },
                                    { $lte: ["$leaveEnd", stopDate] }
                                 ]
                              }, then: {
                                 $divide: [{ $subtract: ["$leaveEnd", "$leaveStart"] }, mSecInDay]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $gte: ["$leaveStart", startDate] },
                                    { $lte: ["$leaveStart", stopDate] },
                                    { $gte: ["$leaveEnd", startDate] },
                                    { $gte: ["$leaveEnd", stopDate] }
                                 ]
                              }, then: {
                                 $divide: [{ $subtract: [stopDate, "$leaveStart"] }, mSecInDay]
                              }
                           }
                        ]
                     }
                  }
               }
            },
            {
               $group: {
                  "_id": "$cityCode",
                  "totalDays": { "$sum": "$calcDays" }
               }
            }
         ]).toArray((err, leaveDaysObj) => {
            if (err) {
               reject("DB error in " + countLocationHolidays.name + ": " + err);
            } else if (leaveDaysObj.length >= 1) {
               resolve(leaveDaysObj[0].totalDays);
            } else {
               resolve(0);
            }
         });
      }
   });
}


function getLocationLeave(cityCode, revenueYear) {
   return new Promise((resolve, reject) => {
      if (cityCode === undefined || cityCode === "") {
         reject(getLocationLeave.name + ": Location is not provided");
      } else if (revenueYear === undefined || revenueYear === "") {
         reject(getLocationLeave.name + ": Revenue year is not provided");
      } else {
         let revenueStart = new Date(revenueYear, 0, 1);
         revenueStart.setHours(0, 0, 0, 0);
         let revenueEnd = new Date(revenueYear, 12, 0);
         revenueEnd.setHours(0, 0, 0, 0);
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
                  "days": { $toInt: "$days" },
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
               commObj.computeLeaveDays(locLeaveArr).then((allDaysInLeave) => {
                  commObj.computeWeekdaysInLeave(locLeaveArr).then((workDaysInLeave) => {
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

module.exports = {
   getLocationLeave,
   countLocationHolidays
}