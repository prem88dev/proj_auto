const dbObj = require("./database");
const dateTime = require("date-and-time");
const dateFormat = require("dateformat");
const locLeaveColl = "loc_holiday";
const monthFirstDate = 1;
const monthLastDate = 0;

/*
   getEmployeeLeave:
      this function will return array of either personal
      or location leaves based on personal flag

   params:
      input:
         empEsaLink - inker between employee and project
         ctsEmpId - employee id
         revenueYear - year for which leaves are required
         personal - boolean flag
            if true, will fetch personal leaves
            else, will fetch location holidays

      return an arry of leaves for the given revenue year
*/
function getLocationLeave(wrkCity, revenueYear, monthIndex) {
   return new Promise((resolve, reject) => {
      db = dbObj.getDb();
      let leaveYear = parseInt(revenueYear, 10);
      let startMonth = parseInt(monthIndex, 10);
      let endMonth = parseInt(monthIndex, 10) + 1;
      let revenueStart = new Date(leaveYear, startMonth, monthFirstDate);
      let revenueEnd = new Date(leaveYear, endMonth, monthLastDate);
      db.collection(locLeaveColl).aggregate([
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
               "wrkCity": "$wrkCity",
               "startDate": "$leaveStart",
               "endDate": "$leaveEnd",
               "days": "$days",
               "description": "$description"
            }
         }
      ]).toArray((err, locLeaveArr) => {
         if (err) {
            reject(err);
         } else {
            resolve(locLeaveArr);
         }
      });
   });
}

module.exports = {
   getLocationLeave
}