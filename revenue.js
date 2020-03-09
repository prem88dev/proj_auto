const dbObj = require('./database');
const commObj = require('./utility');
const dateTime = require("date-and-time");
const dateFormat = require("dateformat");
const printf = require('printf');

const empMonthRevColl = "emp_month_revenue";

function calcMonthLeaves(empLeaveArrObj, monthIndex, revMonthStartDate, revMonthEndDate) {
   return new Promise((resolve, _reject) => {
      let leaveDays = 0;
      for (let idx = 0; idx < (empLeaveArrObj.length - 1); idx++) {
         let leave = empLeaveArrObj[idx];
         if (leave.startDate !== undefined && leave.startDate != "" && leave.endDate !== undefined && leave.endDate != "") {
            let leaveStart = new Date(leave.startDate);
            var leaveEnd = new Date(leave.endDate);
            if (monthIndex === leaveStart.getMonth() && monthIndex !== leaveEnd.getMonth()) {
               leaveEnd = revMonthEndDate;
            } else if (monthIndex === leaveEnd.getMonth()) {
               leaveStart = revMonthStartDate;
            }

            commObj.getDaysBetween(leaveStart, leaveEnd, true).then((daysBetween) => {
               leaveDays += parseInt(daysBetween, 10);
            });
         }
      }
      resolve(leaveDays);
   });
}


function calcProjectionRevenueDays(empJsonObj, monthIndex, revMonthStartDate, revMonthEndDate) {
   return new Promise((resolve, _reject) => {
      commObj.getDaysBetween(revMonthStartDate, revMonthEndDate, true).then((weekDays) => {
         calcMonthBuffers(empJsonObj[2].buffers, monthIndex).then((bufferDays) => {
            calcMonthLeaves(empJsonObj[3].publicHolidays, monthIndex, revMonthStartDate, revMonthEndDate).then((locationLeaves) => {
               resolve(weekDays - (bufferDays + locationLeaves));
            });
         });
      });
   });
}


function calcMonthBuffers(empBufferArrObj, monthIndex) {
   return new Promise((resolve, _reject) => {
      let bufferDays = 0;
      for (let idx = 0; idx < (empBufferArrObj.length - 1); idx++) {
         let buffer = empBufferArrObj[idx];
         let bufferMonth = new Date(dateTime.parse(buffer.month, "MMYYYY", true)).getMonth();
         if (monthIndex === bufferMonth) {
            bufferDays += parseInt(buffer.days, 10);
         }
      }
      resolve(bufferDays);
   });
}


function updEmpMonthRevenue(empJsonObj, monthRevArr) {
   return new Promise((resolve, _reject) => {
      let empEsaLink = empJsonObj[0].empEsaLink;
      let ctsEmpId = empJsonObj[0].ctsEmpId;
      let month = monthRevArr.month;
      let startDate = dateFormat((new Date(dateTime.parse(monthRevArr.startDate, "DD-MMM-YYYY", true))), "ddmmyyyy");
      let endDate = dateFormat((new Date(dateTime.parse(monthRevArr.endDate, "DD-MMM-YYYY", true))), "ddmmyyyy");
      let cmiRevenue = monthRevArr.cmiRevenue;
      let monthRevenue = monthRevArr.monthRevenue;
      let empMonthRevObj = { 'empEsaLink': empEsaLink, 'ctsEmpId': ctsEmpId, 'month': month, 'startDate': startDate, 'endDate': endDate, 'monthRevenue': monthRevenue, 'cmiRevenue': cmiRevenue };
      db = dbObj.getDb();
      db.collection(empMonthRevColl).updateOne({ 'empEsaLink': empEsaLink, 'ctsEmpId': ctsEmpId, 'month': month }, { $set: empMonthRevObj }, { upsert: true }).then((result) => {
         resolve(result);
      });
   });
}


function getEmpMonthlyRevenue(empJsonObj, revenueYear, monthIndex, revMonthStartDate, revMonthEndDate) {
   return new Promise((resolve, _reject) => {
      let billRatePerHr = parseInt(empJsonObj[0].billRatePerHr, 10);
      let billHourPerDay = parseInt(empJsonObj[0].wrkHrPerDay, 10);
      let revenueMonth = printf("%02s%04s", monthIndex + 1, parseInt(revenueYear, 10));
      let revenueAmount = 0;
      let cmiRevenue = 0;
      calcProjectionRevenueDays(empJsonObj, monthIndex, revMonthStartDate, revMonthEndDate).then((revenueDays) => {
         cmiRevenue = ((parseInt(revenueDays, 10) * billHourPerDay * billRatePerHr));
         let monthRevenueObj = "";
         let monthStartDate = new Date(revenueYear, monthIndex, 1);
         let monthEndDate = new Date(revenueYear, parseInt((monthIndex + 1), 10), 0);
         revenueAmount = (revenueDays * billHourPerDay * billRatePerHr);
         monthRevenueObj = { 'month': revenueMonth, 'startDate': dateFormat(monthStartDate, "dd-mmm-yyyy"), 'endDate': dateFormat(monthEndDate, "dd-mmm-yyyy"), 'monthRevenue': revenueAmount, 'cmiRevenue': cmiRevenue };
         updEmpMonthRevenue(empJsonObj, monthRevenueObj).then((result) => {
            /*const { matchedCount, modifiedCount, upsertedCount } = result;
            console.log("matchedCount: " + matchedCount + "    modifiedCount: " + modifiedCount + "    upsertedCount: " + upsertedCount);*/
         });
         resolve(monthRevenueObj);
      });
   });
}



function calcEmpRevenue(empJsonObj, revenueYear) {
   let monthRevArr = [];
   return new Promise((resolve, reject) => {
      if (empJsonObj === undefined) {
         reject(calcEmpRevenue.name + ": Employee object is not provided");
      } else if (revenueYear === undefined) {
         reject(calcEmpRevenue.name + ": Revenue year is not provided");
      } else if (empJsonObj[0].sowStartDate === undefined || empJsonObj[0].sowStartDate == "") {
         reject(calcEmpRevenue.name + ": SOW start date is not defined for selected employee");
      } else if (empJsonObj[0].sowEndDate === undefined || empJsonObj[0].sowEndDate == "") {
         reject(calcEmpRevenue.name + ": SOW end date is not defined for selected employee");
      } else if (empJsonObj[0].billRatePerHr === undefined || empJsonObj[0].billRatePerHr == "") {
         reject(calcEmpRevenue.name + ": Billing hour per day is not defined for selected employee");
      } else if (empJsonObj[0].wrkHrPerDay === undefined || empJsonObj[0].wrkHrPerDay == "") {
         reject(calcEmpRevenue.name + ": Billing rate is not defined for selected employee");
      }

      let sowStart = new Date(dateTime.parse(empJsonObj[0].sowStartDate, "DDMMYYYY", true));
      let sowEnd = new Date(dateTime.parse(empJsonObj[0].sowEndDate, "DDMMYYYY", true));

      for (let monthIndex = 0; monthIndex <= 11; monthIndex++) {
         let revMonthStartDate = new Date(revenueYear, monthIndex, 1);
         let revMonthEndDate = new Date(revenueYear, monthIndex + 1, 0);
         if (sowStart.getFullYear() === parseInt(revenueYear, 10)) {
            if (sowStart.getMonth() === monthIndex) {
               revMonthStartDate = sowStart;
            }
         }

         if (sowEnd.getFullYear() === parseInt(revenueYear, 10)) {
            if (sowEnd.getMonth() === monthIndex) {
               revMonthEndDate = sowEnd;
            }
         }
         if (revMonthStartDate >= sowStart && revMonthEndDate <= sowEnd) {
            monthRevArr.push(getEmpMonthlyRevenue(empJsonObj, revenueYear, monthIndex, revMonthStartDate, revMonthEndDate));
         } else {
            let revenueMonth = printf("%02s%04s", monthIndex + 1, parseInt(revenueYear, 10));
            let monthRevenueObj = { 'month': revenueMonth, 'startDate': dateFormat(revMonthStartDate, "dd-mmm-yyyy"), 'endDate': dateFormat(revMonthEndDate, "dd-mmm-yyyy"), 'monthRevenue': 0, 'cmiRevenue': 0 };
            monthRevArr.push(monthRevenueObj);
         }
      }
      Promise.all(monthRevArr).then((monthRevenue) => {
         resolve(monthRevenue);
      });
   });
}

module.exports = {
   calcEmpRevenue,
   getEmpMonthlyRevenue
}