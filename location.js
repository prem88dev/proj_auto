const dbObj = require('./database');
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
      let revenueStart = new Date(revenueYear, parseInt(monthIndex), 2);
      console.log(revenueStart);
      let revenueEnd = new Date(revenueYear, parseInt(monthIndex) + 1, 1);
      console.log(revenueEnd);
      db = dbObj.getDb();
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
               "wrkCity": wrkCity,
               /*
                  consider a sample year 2018 with
                  revenue start date: 01-Jan-2018 and
                  revenue end date: 31-Dec-2018
               */
               "$or": [
                  {
                     /* 
                        leave starts on or ahead of 01-Jan-2018 and ends on or after 31-Dec-2018

                        success scneario exclusive to below filter
                           +===============+===============+
                           |  leave start  |   leave end   |
                           |     date      |     date      |
                           +---------------|---------------+ 
                           |  03-Dec-2017  |  03-Mar-2019 ==> highly impossible
                           |  03-Dec-2017  |  31-Dec-2018  |
                           |  01-Jan-2018  |  03-Mar-2019  |
                           |  01-Jan-2018  |  31-Dec-2018  |
                           +===============+===============+
                     */
                     "$and": [
                        { "leaveStart": { "$lte": revenueStart } },
                        { "leaveEnd": { "$gte": revenueEnd } }
                     ]
                  },
                  {
                     /*
                        leave starts on or ahead of 01-Jan-2018 (can be like, 04-Sep-2016 or 31-Dec-2017 or 01-Jan-2018),
                        but ends within revenue year (i.e., between 01-Jan-2018 and 31-Dec-2018).

                        success scneario exclusive to below filter
                           +===============+===============+
                           |  leave start  |   leave end   |
                           |     date      |     date      |
                           +---------------|---------------+
                           |  03-Dec-2017  |  03-Mar-2018  |
                           |  01-Jan-2018  |  03-Mar-2018  |
                           +===============+===============+
                     */
                     "$and": [
                        { "leaveStart": { "$gte": revenueStart } },
                        { "leaveEnd": { "$gte": revenueStart } },
                        { "leaveEnd": { "$lte": revenueEnd } }
                     ]
                  },
                  {
                     /*
                        leave starts on or ahead of 01-Jan-2018 (can be like, 04-Sep-2016 or 31-Dec-2017 or 01-Jan-2018),
                        but ends within revenue year (i.e., between 01-Jan-2018 and 31-Dec-2018).

                        success scneario exclusive to below filter
                           +===============+===============+
                           |  leave start  |   leave end   |
                           |     date      |     date      |
                           +---------------|---------------+
                           |  03-Dec-2017  |  03-Mar-2018  |
                           |  01-Jan-2018  |  03-Mar-2018  |
                           +===============+===============+
                     */
                     "$and": [
                        { "leaveStart": { "$lte": revenueStart } },
                        { "leaveEnd": { "$gte": revenueStart } },
                        { "leaveEnd": { "$lte": revenueEnd } }
                     ]
                  },
                  {
                     /*
                        leave starts within revenue year (should be within 01-Jan-2018 to 31-Dec-2018),
                        but ends on of after 31-Dec-2018.

                        success scneario exclusive to below filter
                           +===============+===============+
                           |  leave start  |   leave end   |
                           |     date      |     date      |
                           +---------------|---------------+ 
                           |  31-Dec-2018  |  05-Jan-2019  |
                           |  03-Mar-2018  |  31-Dec-2018  |
                           +===============+===============+
                     */
                     "$and": [
                        { "leaveStart": { "$gte": revenueStart } },
                        { "leaveStart": { "$lte": revenueEnd } },
                        { "leaveEnd": { "$gte": revenueEnd } }
                     ]
                  },
                  {
                     /*
                        leave starts within revenue year (should be within 01-Jan-2018 to 31-Dec-2018),
                        but ends on of after 31-Dec-2018.

                        success scneario exclusive to below filter
                           +===============+===============+
                           |  leave start  |   leave end   |
                           |     date      |     date      |
                           +---------------|---------------+ 
                           |  31-Dec-2018  |  05-Jan-2019  |
                           |  03-Mar-2018  |  31-Dec-2018  |
                           +===============+===============+
                     */
                     "$and": [
                        { "leaveStart": { "$gte": revenueStart } },
                        { "leaveStart": { "$lte": revenueEnd } },
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
               "startDate": "$startDate",
               "endDate": "$endDate",
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