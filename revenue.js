const commObj = require("./utility");
const dateTime = require("date-and-time");
const dateFormat = require("dateformat");
const printf = require('printf');


function calcMonthLeaves(leaveArrObj, monthIndex, revMonthStartDate, revMonthEndDate) {
   return new Promise(async (resolve, reject) => {
      if (leaveArrObj === undefined || leaveArrObj === "") {
         reject(calcMonthLeaves.name + ": Employee leave array is not provided");
      } else if (monthIndex === undefined || monthIndex === "") {
         reject(calcMonthLeaves.name + ": Month index is not provided");
      } else if (revMonthStartDate === undefined || revMonthStartDate === "") {
         reject(calcMonthLeaves.name + ": Revenue month start date is not provided");
      } else if (revMonthEndDate === undefined || revMonthEndDate === "") {
         reject(calcMonthLeaves.name + ": Revenue month end date is not provided");
      } else {
         let leaveDays = 0;
         let totalObjs = (leaveArrObj.length - 1);
         await leaveArrObj.forEach((leaveObj, idx) => {
            if (idx < totalObjs) {
               if (leaveObj.startDate !== undefined && leaveObj.startDate != "" && leaveObj.endDate !== undefined && leaveObj.endDate != "") {
                  let leaveStart = new Date(leaveObj.startDate);
                  leaveStart.setUTCHours(0, 0, 0, 0);
                  let leaveEnd = new Date(leaveObj.endDate);
                  leaveEnd.setUTCHours(0, 0, 0, 0);

                  commObj.getDaysBetween(leaveStart, leaveEnd, true).then((daysBetween) => {
                     leaveDays += parseInt(daysBetween, 10);
                  });
               }
            }
         });
         resolve(leaveDays);
      }
   });
}


function calcMonthBuffers(empBufferArrObj, monthIndex) {
   return new Promise(async (resolve, reject) => {
      if (empBufferArrObj === undefined || empBufferArrObj === "") {
         reject(calcMonthBuffers.name + ": Buffer array is not defined");
      } else if (monthIndex === undefined || monthIndex === "") {
         reject(calcMonthBuffers.name + ": Month index is not provided");
      } else {
         let bufferDays = 0;
         let bufferObjs = (empBufferArrObj.length - 1);
         await empBufferArrObj.forEach((empBuffer, idx) => {
            if (idx < bufferObjs) {
               let bufferMonth = new Date(dateTime.parse(empBuffer.month, "MMM-YYYY", true)).getMonth();
               if (monthIndex === bufferMonth) {
                  bufferDays += parseInt(empBuffer.days, 10);
               }
            }
         });
         resolve(bufferDays);
      }
   });
}



function getEmpMonthlyRevenue(empJsonObj, revenueYear, monthIndex, revMonthStartDate, revMonthStopDate) {
   return new Promise((resolve, reject) => {
      if (empJsonObj === undefined || empJsonObj === "") {
         reject(calcMonthLeaves.name + ": Employee leave array is not provided");
      } else if (revenueYear === undefined || revenueYear === "") {
         reject(calcMonthLeaves.name + ": Month index is not provided");
      } else if (monthIndex === undefined || monthIndex === "") {
         reject(calcMonthLeaves.name + ": Month index is not provided");
      } else if (revMonthStartDate === undefined || revMonthStartDate === "") {
         reject(calcMonthLeaves.name + ": Revenue month start date is not provided");
      } else if (revMonthStopDate === undefined || revMonthStopDate === "") {
         reject(calcMonthLeaves.name + ": Revenue month end date is not provided");
      } else {
         let billRatePerHr = parseInt(empJsonObj[0].billRatePerHr, 10);
         let billHourPerDay = parseInt(empJsonObj[0].wrkHrPerDay, 10);
         let revenueMonth = printf("%02s%04s", monthIndex + 1, parseInt(revenueYear, 10));
         let revenueStartDate = new Date(revMonthStartDate);
         revenueStartDate.setHours(0, 0, 0, 0);
         let revenueStopDate = new Date(revMonthStopDate);
         revenueStopDate.setHours(0, 0, 0, 0);
         let monthStartDate = new Date(revenueYear, monthIndex, 1);
         monthStartDate.setHours(0, 0, 0, 0);
         let monthEndDate = new Date(revenueYear, parseInt((monthIndex + 1), 10), 0);
         monthEndDate.setHours(0, 0, 0, 0);
         let sowStart = new Date(dateTime.parse(empJsonObj[0].sowStartDate, "D-MMM-YYYY", true));
         sowStart.setHours(0, 0, 0, 0);
         let sowEnd = new Date(dateTime.parse(empJsonObj[0].sowEndDate, "D-MMM-YYYY", true));
         sowEnd.setHours(0, 0, 0, 0);
         commObj.getDaysBetween(revMonthStartDate, revMonthStopDate, true).then((workDays) => {
            let empEsaLink = empJsonObj[0].empEsaLink;
            let ctsEmpId = `${empJsonObj[0].ctsEmpId}`;
            commObj.countPersonalDays(empEsaLink, ctsEmpId, revMonthStartDate, revMonthStopDate).then((personalDays) => {
               commObj.countLocationHolidays(empJsonObj[0].cityCode, revMonthStartDate, revMonthStopDate).then((locationLeaves) => {
                  calcMonthBuffers(empJsonObj[2].buffers, monthIndex).then((bufferDays) => {
                     let monthRevenue = 0;
                     let cmiRevenue = 0;
                     let monthRevenueObj = {};
                     let weekDays = parseInt(workDays, 10);
                     let locationHoliday = parseInt(locationLeaves, 10);
                     let selfDays = parseInt(personalDays, 10);
                     let buffer = parseInt(bufferDays, 10);
                     let revenueDays = weekDays - (locationHoliday + selfDays + buffer);
                     let cmiRevenueDays = weekDays - locationHoliday;
                     let revenueStartDateSlack = 0;
                     let revenueStopDateSlack = 0;
                     if (revenueStartDate.getFullYear() === sowStart.getFullYear() && revenueStartDate.getMonth() === sowStart.getMonth()) {
                        if (revenueStartDate.getDate() > sowStart.getDate()) {
                           revenueStartDateSlack = revenueStartDate.getDate() - sowStart.getDate();
                        } else if (revenueStartDate.getDate() < sowStart.getDate()) {
                           revenueStartDateSlack = sowStart.getDate() - revenueStartDate.getDate();
                        }
                     }
                     if (revenueStopDate.getFullYear() === sowEnd.getFullYear() && revenueStopDate.getMonth() === sowEnd.getMonth()) {
                        if (revenueStopDate.getDate() > sowEnd.getDate()) {
                           revenueStopDateSlack = revenueStopDate.getDate() - sowEnd.getDate();
                        } else if (revenueStopDate.getDate() < sowEnd.getDate()) {
                           revenueStopDateSlack = sowEnd.getDate() - revenueStopDate.getDate();
                        }
                     }
                     let totalRevenueDays = revenueDays;
                     let totalCmiDays = cmiRevenueDays;
                     if (revenueStartDate.getFullYear() >= sowStart.getFullYear() && revenueStopDate.getFullYear() <= sowEnd.getFullYear()) {
                        if (revenueStartDate.getMonth() === sowStart.getMonth()) {
                           totalRevenueDays = revenueDays - revenueStartDateSlack;
                           totalCmiDays = cmiRevenueDays - revenueStartDateSlack;
                        }
                        if (revenueStopDate.getMonth() === sowEnd.getMonth()) {
                           totalRevenueDays = revenueDays - revenueStopDateSlack;
                           totalCmiDays = cmiRevenueDays - revenueStopDateSlack;
                        }
                        monthRevenue = totalRevenueDays * billHourPerDay * billRatePerHr;
                        cmiRevenue = totalCmiDays * billHourPerDay * billRatePerHr;
                     }
                     monthRevenueObj = { 'month': dateFormat(dateTime.parse(revenueMonth, "MMYYYY", true), "mmm-yyyy"), 'startDate': dateFormat(monthStartDate, "d-mmm-yyyy"), 'endDate': dateFormat(monthEndDate, "d-mmm-yyyy"), 'weekDays': weekDays, 'locationHolidays': locationHoliday, 'selfDays': selfDays, 'bufferDays': buffer, 'monthRevenue': monthRevenue, 'cmiRevenue': cmiRevenue };
                     resolve(monthRevenueObj);
                  }).catch((calcMonthBuffersErr) => { reject(calcMonthBuffersErr); });
               }).catch((countLocationHolidaysErr) => { reject(countLocationHolidaysErr); });
            }).catch((countPersonalDaysErr) => { reject(countPersonalDaysErr); });
         });
      }
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
      } else {
         let sowStart = new Date(dateTime.parse(empJsonObj[0].sowStartDate, "DDMMYYYY", true));
         sowStart.setUTCHours(0, 0, 0, 0);
         let sowEnd = new Date(dateTime.parse(empJsonObj[0].sowEndDate, "DDMMYYYY", true));
         sowEnd.setUTCHours(0, 0, 0, 0);

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
            monthRevArr.push(getEmpMonthlyRevenue(empJsonObj, revenueYear, monthIndex, revMonthStartDate, revMonthEndDate));
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