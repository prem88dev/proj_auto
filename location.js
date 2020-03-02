const dbObj = require('./database');
const commObj = require('./utility');
const locLeaveColl = "loc_holiday";

function getLocationLeave(wrkCity, revenueYear) {
   let revenueStart = new Date(revenueYear, 0, 2);
   revenueStart.setUTCHours(0, 0, 0, 0);
   let revenueEnd = new Date(revenueYear, 12, 1);
   revenueEnd.setUTCHours(0, 0, 0, 0);
   return new Promise((resolve, reject) => {
      dbObj.getDb().collection(locLeaveColl).aggregate([
         {
            $project: {
               "_id": 1,
               "wrkCity": 2,
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
               "wrkCity": { "$eq": wrkCity },
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
            reject(err);
         } else if (locLeaveArr.length >= 1) {
            commObj.computeLeaveDays(locLeaveArr).then((allDaysInLeave) => {
               commObj.computeWeekdaysInLeave(locLeaveArr).then((workDaysInLeave) => {
                  locLeaveArr.push({ 'totalDays': allDaysInLeave, 'workDays': workDaysInLeave });
                  resolve(locLeaveArr);
               });
            });
         }
      });
   });
}

module.exports = {
   getLocationLeave
}