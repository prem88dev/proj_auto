const dbObj = require('./database');
const empLeaveColl = "emp_leave";
const empBuffer = "emp_buffer";
const empProjColl = "emp_proj";


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
function getPersonalLeave(empEsaLink, ctsEmpId, revenueYear) {
   return new Promise((resolve, reject) => {
      let revenueStart = new Date(revenueYear, 1, monthFirstDate);
      console.log(revenueStart);
      let revenueEnd = new Date(revenueYear, 12, monthLastDate);
      console.log(revenueEnd);
      db = dbObj.getDb();
      let myCol = "";
      if (personal === true) { /* if personal flag is set */
         myCol = empLeaveColl; /* fetch personal leaves */
      } else {
         myCol = locLeaveColl; /* fetch location leaves */
      }
      db.collection(myCol).aggregate([
         {
            $project: {
               "_id": 1,
               "empEsaLink": 3,
               "ctsEmpId": 4,
               "startDate": 5,
               "endDate": 6,
               "days": 7,
               "reason": 8,
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
               "empEsaLink": empEsaLink,
               "ctsEmpId": ctsEmpId,
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
                  }
               ]
            }
         },
         {
            $project: {
               "_id": "$_id",
               "empEsaLink": "$empEsaLink",
               "ctsEmpId": "$ctsEmpId",
               "startDate": "$startDate",
               "endDate": "$endDate",
               "days": "$days",
               "reason": "$reason"
            }
         }
      ]).toArray((err, leaveArr) => {
         if (err) {
            reject(err);
         } else {
            resolve(leaveArr);
         }
      });
   });
}



/*
   getEmployeeLeave:
      this function will return array of either personal
      or location leaves based on personal flag

   params:
      input:
         empEsaLink - inker between employee and project
         ctsEmpId - employee id
         revenueYear - year for which leaves are required
      return an arry of leaves for the given revenue year
*/
function getEmployeeBuffer(empEsaLink, ctsEmpId, revenueYear) {
   return new Promise((resolve, reject) => {
      let revenueStart = new Date(revenueYear, 1, monthFirstDate);
      console.log(revenueStart);
      let revenueEnd = new Date(revenueYear, 12, monthLastDate);
      console.log(revenueEnd);
      db = dbObj.getDb();
      db.collection(empBuffer).aggregate([
         {
            $project: {
               "_id": 1,
               "empEsaLink": 3,
               "ctsEmpId": 4,
               "month": 5,
               "days": 6,
               "reason": 7,
               "bufferDate": {
                  $dateFromString: {
                     dateString: "$month",
                     format: "01" + "%m%Y"
                  }
               }
            }
         },
         {
            $match: {
               "empEsaLink": empEsaLink,
               "ctsEmpId": ctsEmpId,
               "$and": [
                  { "bufferDate": { "$gte": revenueStart } },
                  { "bufferDate": { "$lte": revenueEnd } }
               ]
            }
         },
         {
            $project: {
               "_id": "$_id",
               "empEsaLink": "$empEsaLink",
               "ctsEmpId": "$ctsEmpId",
               "startDate": "$startDate",
               "endDate": "$endDate",
               "days": "$days",
               "reason": "$reason"
            }
         }
      ]).toArray((err, leaveArr) => {
         if (err) {
            reject(err);
         } else {
            resolve(leaveArr);
         }
      });
   });
}



/* get projection data for specific employee in selected project */
function getEmployeeProjection(empEsaLink, ctsEmpId, revenueYear) {
   console.log(empEsaLink + " - " + ctsEmpId + " - " + revenueYear);
   return new Promise((resolve, reject) => {
      db = getDb();
      db.collection(empProjColl).aggregate([
         {
            $lookup: {
               from: "esa_proj",
               localField: "empEsaLink",
               foreignField: "empEsaLink",
               as: "empEsaProj"
            }
         },
         {
            $unwind: "$empEsaProj"
         },
         {
            $lookup: {
               from: "wrk_loc",
               localField: "wrkCity",
               foreignField: "wrkCity",
               as: "empEsaLoc"
            }
         },
         {
            $unwind: "$empEsaLoc"
         },
         {
            $lookup: {
               from: "emp_leave",
               localField: "ctsEmpId",
               foreignField: "ctsEmpId",
               as: "empEsaLeave"
            }
         },
         {
            $unwind: "$empEsaLeave"
         },
         {
            $lookup: {
               from: "emp_buffer",
               localField: "ctsEmpId",
               foreignField: "ctsEmpId",
               as: "empEsaBuffer"
            }
         },
         {
            $unwind: "$empEsaBuffer"
         },
         {
            $lookup: {
               from: "loc_holiday",
               localField: "wrkCity",
               foreignField: "wrkCity",
               as: "empLocHoliday"
            }
         },
         {
            $unwind: "$empLocHoliday"
         },
         {
            $match: {
               "empEsaLink": empEsaLink,
               "ctsEmpId": ctsEmpId
            }
         },
         {
            $group: {
               "_id": "$_id",
               "esaId": { "$first": "$empEsaProj.esaId" },
               "esaDesc": { "$first": "$empEsaProj.esaDesc" },
               "projName": { "$first": "$projName" },
               "ctsEmpId": { "$first": "$ctsEmpId" },
               "empFname": { "$first": "$empFname" },
               "empMname": { "$first": "$empMname" },
               "empLname": { "$first": "$empLname" },
               "lowesUid": { "$first": "$lowesUid" },
               "deptName": { "$first": "$deptName" },
               "sowStartDate": { "$first": "$sowStartDate" },
               "sowEndDate": { "$first": "$sowEndDate" },
               "foreseenEndDate": { "$first": "$foreseenEndDate" },
               "wrkCity": { "$first": "$empEsaLoc.cityName" },
               "wrkHrPerDay": { "$first": "$wrkHrPerDay" },
               "billRatePerHr": { "$first": "$billRatePerHr" },
               "empEsaLink": { "$first": "$empEsaLink" },
               "projectionActive": { "$first": "$projectionActive" },
               "leaves": {
                  "$addToSet": {
                     "_id": "$empEsaLeave._id",
                     "startDate": "$empEsaLeave.startDate",
                     "endDate": "$empEsaLeave.endDate",
                     "days": "$empEsaLeave.days",
                     "reason": "$empEsaLeave.reason"
                  }
               },
               "buffers": {
                  "$addToSet": {
                     "_id": "$empEsaBuffer._id",
                     "month": "$empEsaBuffer.month",
                     "days": "$empEsaBuffer.days",
                     "reason": "$empEsaBuffer.reason"
                  }
               },
               "publicHolidays": {
                  "$addToSet": {
                     "_id": "$empLocHoliday._id",
                     "startDate": "$empLocHoliday.startDate",
                     "endDate": "$empLocHoliday.endDate",
                     "days": "$empLocHoliday.days",
                     "description": "$empLocHoliday.description"
                  }
               }
            }
         }
      ]).toArray((err, empDtl) => {
         if (empDtl.length === 1) {
            calcEmpRevenue(empDtl, revenueYear).then((revenueDetail) => {
               empDtl.push({ "revenue": revenueDetail });
               if (err) {
                  reject(err);
               } else {
                  resolve(empDtl);
               }
            });
         } else {
            reject("")
         }
      });
   });
}



module.exports = {
   getPersonalLeave,
   getEmployeeBuffer,
   getEmployeeProjection
}