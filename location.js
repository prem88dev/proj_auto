const dbObj = require('./database');
const commObj = require('./utility');
const locLeaveColl = "loc_holiday";


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
                  $or: [
                     {
                        $and: [
                           { "leaveStart": { "$lte": revenueStart } },
                           { "leaveEnd": { "$gte": revenueEnd } }
                        ]
                     },
                     {
                        $and: [
                           { "leaveStart": { "$lte": revenueStart } },
                           { "leaveEnd": { "$gte": revenueStart } },
                           { "leaveEnd": { "$lte": revenueEnd } }
                        ]
                     },
                     {
                        $and: [
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
   getLocationLeave
}