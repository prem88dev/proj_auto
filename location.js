const dbObj = require('./database');
const commObj = require('./utility');
const locLeaveColl = "loc_holiday";

function computeWeekdaysInLeave(leaveArr) {
   let weekdaysInLeave = 0;
   return new Promise(async (resolve, _reject) => {
      await leaveArr.forEach((leave) => {
         commObj.getDaysBetween(leave.startDate, leave.endDate, true).then((weekdays) => {
            weekdaysInLeave += weekdays;
         });
      });
      resolve(weekdaysInLeave);
   });
}

function computeLeaveDays(leaveArr) {
   let leaveDays = 0;
   return new Promise(async (resolve, _reject) => {
      await leaveArr.forEach((leave) => {
         commObj.getDaysBetween(leave.startDate, leave.endDate, false).then((daysBetween) => {
            leaveDays += daysBetween;
         });
      });
      resolve(leaveDays);
   });
}


function countLocationHolidays(cityCode, locLeaveStart, locLeaveStop) {
   return new Promise((resolve, reject) => {
      if (cityCode === undefined || cityCode === "") {
         reject(getPersonalLeave.name + ": Linker ID is not provided");
      } else if (locLeaveStart === undefined || locLeaveStart === "") {
         reject(getPersonalLeave.name + ": Leave start date is not provided");
      } else if (locLeaveStop === undefined || locLeaveStop === "") {
         reject(getPersonalLeave.name + ": Leave end date is not provided");
      } else {
         let leaveBegin = new Date(locLeaveStart);
         leaveBegin.setHours(0, 0, 0, 0);
         let leaveDone = new Date(locLeaveStop);
         leaveDone.setHours(0, 0, 0, 0);
         dbObj.getDb().collection(locLeaveColl).aggregate([
            {
               $project: {
                  "_id": 1,
                  "cityCode": 2,
                  "startDate": 3,
                  "endDate": 4,
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
                           { "leaveStart": { "$lte": leaveBegin } },
                           { "leaveEnd": { "$gte": leaveDone } }
                        ]
                     },
                     {
                        "$and": [
                           { "leaveStart": { "$lte": leaveBegin } },
                           { "leaveEnd": { "$gte": leaveBegin } },
                           { "leaveEnd": { "$lte": leaveDone } }
                        ]
                     },
                     {
                        "$and": [
                           { "leaveStart": { "$gte": leaveBegin } },
                           { "leaveStart": { "$lte": leaveDone } },
                           { "leaveEnd": { "$gte": leaveBegin } },
                           { "leaveEnd": { "$lte": leaveDone } }
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
                  "calcDays": {
                     $switch: {
                        branches: [
                           { case: { $eq: ["$leaveStart", "$leaveEnd"] }, "then": 1 },
                           {
                              case: {
                                 $and: [
                                    { $lte: ["leaveStart", leaveBegin] },
                                    { $gte: ["leaveEnd", leaveDone] }
                                 ]
                              }, then: {
                                 $add: [{ $subtract: [leaveBegin, leaveDone] }, 1]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $lte: ["leaveStart", leaveBegin] },
                                    { $gte: ["leaveEnd", leaveBegin] },
                                    { $lte: ["leaveEnd", leaveDone] }
                                 ]
                              }, then: {
                                 $add: [{ $subtract: [leaveBegin, "$leaveEnd"] }, 1]
                              }
                           },
                           {
                              case: {
                                 $and: [
                                    { $gte: ["leaveStart", leaveBegin] },
                                    { $lte: ["leaveStart", leaveDone] },
                                    { $gte: ["leaveEnd", leaveBegin] },
                                    { $lte: ["leaveEnd", leaveDone] }
                                 ]
                              }, then: {
                                 $add: [{ $subtract: ["$leaveStart", leaveDone] }, 1]
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
                  "days": "$days",
                  "description": "$description"
               }
            }
         ]).toArray((err, locLeaveArr) => {
            if (err) {
               reject("DB error in " + getLocationLeave.name + " function: " + err);
            } else if (locLeaveArr.length >= 1) {
               resolve(locLeaveArr);
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