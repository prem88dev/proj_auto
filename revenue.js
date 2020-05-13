const commObj = require("./utility");
const splWrkObj = require("./specialWorkday");
const dateTime = require("date-and-time");
const dateFormat = require("dateformat");

var fs = require('fs')
var logger = fs.createWriteStream('log.txt');


function calcMonthLeaves(leaveArrObj, monthIndex, startDate, stopDate) {
   return new Promise(async (resolve, reject) => {
      if (leaveArrObj === undefined || leaveArrObj === "") {
         reject(calcMonthLeaves.name + ": Employee leave array is not provided");
      } else if (monthIndex === undefined || monthIndex === "") {
         reject(calcMonthLeaves.name + ": Month index is not provided");
      } else if (startDate === undefined || startDate === "") {
         reject(calcMonthLeaves.name + ": Revenue month start date is not provided");
      } else if (stopDate === undefined || stopDate === "") {
         reject(calcMonthLeaves.name + ": Revenue month end date is not provided");
      } else {
         let leaveDays = 0;
         await leaveArrObj.forEach((leaveObj, idx) => {
            if (idx < (leaveArrObj.length - 1)) {
               if (leaveObj.startDate !== undefined && leaveObj.startDate != "" && leaveObj.stopDate !== undefined && leaveObj.stopDate != "") {
                  let leaveStart = new Date(leaveObj.startDate);
                  leaveStart.setHours(0, 0, 0, 0);
                  let leaveStop = new Date(leaveObj.stopDate);
                  leaveStop.setHours(25, 59, 59, 0);

                  commObj.getDaysBetween(leaveStart, leaveStop, true).then((daysBetween) => {
                     leaveDays += parseInt(daysBetween, 10);
                  });
               }
            }
         });
         resolve(leaveDays);
      }
   });
}


function getBufferHours(empBufferArr, monthIndex, wrkHrPerDay, callerName) {
   let bufferHours = 0;
   return new Promise(async (resolve, reject) => {
      if (empBufferArr === undefined || empBufferArr === "") {
         reject(getBufferHours.name + ": Buffer array is not defined");
      } else if (monthIndex === undefined || monthIndex === "") {
         reject(getBufferHours.name + ": Month index is not provided");
      } else if (wrkHrPerDay === undefined || wrkHrPerDay === "") {
         reject(getBufferHours.name + ": Billing hours per day is not provided");
      } else {
         await empBufferArr.forEach((empBuffer, idx) => {
            if (idx < (empBufferArr.length - 1)) {
               let bufferMonth = new Date(dateTime.parse(empBuffer.month, "MMM-YYYY", true)).getMonth();
               if (monthIndex === bufferMonth) {
                  bufferHours = parseInt(empBuffer.days, 10) * parseInt(wrkHrPerDay, 10);
               }
            }
         });
         resolve(bufferHours);
      }
   });
}


function computeLeaveHour(leaveArr, leaveDate, wrkHrPerDay, callerName) {
   let funcName = computeLeaveHour.name;
   let leaveHour = 0;
   return new Promise(async (resolve, _reject) => {
      if ((leaveArr.length - 1) > 0) {
         await leaveArr.forEach((leave, idx) => {
            if (idx < (leaveArr.length - 1)) {
               let checkDate = new Date(leaveDate);
               checkDate.setHours(0, 0, 0, 0);
               let leaveStart = new Date(leave.startDate);
               leaveStart.setHours(0, 0, 0, 0);
               let leaveStop = new Date(leave.stopDate);
               leaveStop.setHours(0, 0, 0, 0);
               if (checkDate.getTime() >= leaveStart.getTime() && checkDate.getTime() <= leaveStop.getTime()) {
                  if (leave.halfDay === "Y") {
                     if (leave.leaveHour !== undefined && leave.leaveHour !== "") {
                        leaveHour = leave.leaveHour;
                     } else {
                        leaveHour = 4;
                     }
                  } else {
                     leaveHour = wrkHrPerDay;
                  }
               }
            }
         });
         resolve(leaveHour);
      }
   });
}


const calcRevenueHour = (empJsonObj, startDate, stopDate, revenueHourArr, callerName) => {
   let funcName = calcRevenueHour.name;
   let wrkHrPerDay = 0;
   if (empJsonObj[0].wrkHrPerDay !== undefined && empJsonObj[0].wrkHrPerDay !== "") {
      wrkHrPerDay = parseInt(empJsonObj[0].wrkHrPerDay, 10);
   }
   return new Promise(async (resolve, _reject) => {
      let revenueHour = 0;
      let cmiRevenueHour = 0;
      let calcStartDate = new Date(startDate);
      calcStartDate.setHours(0, 0, 0, 0);
      let calcStopDate = new Date(stopDate);
      calcStopDate.setHours(0, 0, 0, 0);
      let tmpHourArr = { "revenueHour": revenueHour, "cmiRevenueHour": cmiRevenueHour, "nextStartDate": stopDate };

      if (calcStartDate.getTime() <= calcStopDate.getTime()) {
         if (revenueHourArr !== undefined && revenueHourArr !== "") {
            if (revenueHourArr.revenueHour !== undefined && revenueHourArr.revenueHour !== "") {
               revenueHour = parseInt(revenueHourArr.revenueHour, 10);
            }
            if (revenueHourArr.cmiRevenueHour !== undefined && revenueHourArr.cmiRevenueHour !== "") {
               cmiRevenueHour = parseInt(revenueHourArr.cmiRevenueHour, 10);
            }
            if (revenueHourArr.nextStartDate !== undefined && revenueHourArr.nextStartDate !== "") {
               calcStartDate = new Date(revenueHourArr.nextStartDate);
               calcStartDate.setHours(0, 0, 0, 0);
            }
         }

         let dayOfWeek = calcStartDate.getDay();
         let intEmpLeaveHour = 0;
         let intLocLeaveHour = 0;
         let intEmpAddlWrkHour = 0;
         let intLocAddlWrkHour = 0

         await computeLeaveHour(empJsonObj[1].leaves, calcStartDate, wrkHrPerDay, funcName).then((empLeaveHour) => {
            intEmpLeaveHour = parseInt(empLeaveHour, 10);
         });

         await computeLeaveHour(empJsonObj[3].publicHolidays, calcStartDate, wrkHrPerDay, funcName).then((locLeaveHour) => {
            intLocLeaveHour = parseInt(locLeaveHour, 10);
         });

         await computeLeaveHour(empJsonObj[4].specialWorkDays.empSplWrk, calcStartDate, wrkHrPerDay, funcName).then((empAddlWrkHour) => {
            intEmpAddlWrkHour = parseInt(empAddlWrkHour, 10);
         });

         await computeLeaveHour(empJsonObj[4].specialWorkDays.locSplWrk, calcStartDate, wrkHrPerDay, funcName).then((locAddlWrkHour) => {
            intLocAddlWrkHour = parseInt(locAddlWrkHour, 10);
         });

         if (dayOfWeek > 0 && dayOfWeek < 6) { /* week day */
            let effLocWrkHr = (wrkHrPerDay - intLocLeaveHour) + intLocAddlWrkHour;
            if (effLocWrkHr > wrkHrPerDay) {
               effLocWrkHr = wrkHrPerDay;
            }

            let effEmpWrkHr = (wrkHrPerDay - intEmpLeaveHour) + intEmpAddlWrkHour;
            if (effEmpWrkHr > wrkHrPerDay) {
               effEmpWrkHr = wrkHrPerDay;
            }

            if (intLocLeaveHour > 0 && intEmpLeaveHour > 0) {
               if ((effLocWrkHr + effEmpWrkHr) < wrkHrPerDay) {
                  revenueHour += effLocWrkHr + effEmpWrkHr;
                  cmiRevenueHour += effLocWrkHr + effEmpWrkHr;
               } else {
                  revenueHour += wrkHrPerDay;
                  cmiRevenueHour += wrkHrPerDay;
               }
            } else if (intLocLeaveHour > 0) {
               revenueHour += effLocWrkHr;
               cmiRevenueHour += effLocWrkHr;
            } else if (intEmpLeaveHour > 0) {
               revenueHour += effEmpWrkHr;
               cmiRevenueHour += wrkHrPerDay;
            } else {
               revenueHour += wrkHrPerDay;
               cmiRevenueHour += wrkHrPerDay;
            }
         } else if (intLocAddlWrkHour > 0 || intEmpAddlWrkHour > 0) {
            let effLocWrkHr = (wrkHrPerDay - intLocLeaveHour) + intLocAddlWrkHour;
            if (effLocWrkHr > wrkHrPerDay) {
               effLocWrkHr = wrkHrPerDay;
            }

            let effEmpWrkHr = (wrkHrPerDay - intEmpLeaveHour) + intEmpAddlWrkHour;
            if (effEmpWrkHr > wrkHrPerDay) {
               effEmpWrkHr = wrkHrPerDay;
            }

            if ((effLocWrkHr + effEmpWrkHr) >= wrkHrPerDay) {
               revenueHour += wrkHrPerDay;
               cmiRevenueHour += wrkHrPerDay;
            }
         }

         logger.write(funcName + ": " + calcStartDate + " => intEmpLeaveHour: [" + intEmpLeaveHour + "]   intLocLeaveHour: [" + intLocLeaveHour + "]   intEmpAddlWrkHour: [" + intEmpAddlWrkHour + "]   intLocAddlWrkHour: [" + intLocAddlWrkHour + "]\n");
         calcStartDate.setDate(calcStartDate.getDate() + 1);
         tmpHourArr = { "revenueHour": revenueHour, "cmiRevenueHour": cmiRevenueHour, "nextStartDate": calcStartDate };

         if (calcStartDate.getTime() > calcStopDate.getTime()) {
            return resolve(tmpHourArr);
         } else {
            return resolve(calcRevenueHour(empJsonObj, calcStartDate, stopDate, tmpHourArr, funcName));
         }
      }
   });
}


function getEmpMonthlyRevenue(empJsonObj, revenueYear, monthIndex, wrkHrPerDay, billRatePerHr, callerName) {
   let funcName = getEmpMonthlyRevenue.name;
   return new Promise(async (resolve, reject) => {
      if (empJsonObj === undefined || empJsonObj === "") {
         reject(calcMonthLeaves.name + ": Employee leave array is not provided");
      } else if (revenueYear === undefined || revenueYear === "") {
         reject(calcMonthLeaves.name + ": Revenue year is not provided");
      } else if (monthIndex === undefined || monthIndex === "") {
         reject(calcMonthLeaves.name + ": Revenue month is not provided");
      } else {
         let monthFirstDate = new Date(revenueYear, monthIndex, 1);
         monthFirstDate.setHours(0, 0, 0, 0);
         let monthLastDate = new Date(revenueYear, (monthIndex + 1), 0);
         monthLastDate.setHours(0, 0, 0, 0);

         let sowStart = new Date(dateTime.parse(empJsonObj[0].sowStart, "D-MMM-YYYY", true));
         sowStart.setHours(0, 0, 0, 0);
         let sowEnd = new Date(dateTime.parse(empJsonObj[0].sowStop, "D-MMM-YYYY", true));
         sowEnd.setHours(0, 0, 0, 0);

         let effStopDate = sowEnd;
         if (empJsonObj[0].foreseenSowStop !== undefined && empJsonObj[0].foreseenSowStop !== "") {
            let foreseenSowStop = new Date(dateTime.parse(empJsonObj[0].foreseenSowStopDate, "D-MMM-YYYY", true));
            foreseenSowStop.setHours(0, 0, 0, 0);

            if ((foreseenSowStop.getFullYear() === sowEnd.getFullYear()) && (foreseenSowStop.getMonth() > sowEnd.getMonth())) {
               effStopDate = foreseenSowStop;
            }
         }

         let calcStartDate = new Date(monthFirstDate);
         calcStartDate.setHours(0, 0, 0, 0);
         let calcStopDate = new Date(monthLastDate);
         calcStopDate.setHours(0, 0, 0, 0);

         if ((sowStart.getFullYear() === calcStartDate.getFullYear()) && (sowStart.getMonth() === calcStartDate.getMonth())) {
            calcStartDate = sowStart;
         }

         if ((effStopDate.getFullYear() === calcStopDate.getFullYear()) && (effStopDate.getMonth() === calcStopDate.getMonth())) {
            calcStopDate = effStopDate;
         }

         let intBufferHour = 0;
         await getBufferHours(empJsonObj[2].buffers, monthIndex, wrkHrPerDay, funcName).then((bufferHours) => {
            intBufferHour = parseInt(bufferHours, 10);
         })

         await calcRevenueHour(empJsonObj, calcStartDate, calcStopDate, "", funcName).then((revenueHourArr) => {
            let revenueMonth = dateFormat(calcStartDate, "mmm-yyyy");
            let intRevenueHour = parseInt(revenueHourArr.revenueHour, 10) - intBufferHour;
            let intCmiRevenueHour = parseInt(revenueHourArr.cmiRevenueHour, 10);
            let intBillRatePerHr = parseInt(billRatePerHr, 10);
            let revenueAmount = intRevenueHour * intBillRatePerHr;
            let cmiRevenueAmount = intCmiRevenueHour * intBillRatePerHr;
            if (calcStartDate.getTime() >= sowStart.getTime() && calcStopDate.getTime() <= sowEnd.getTime()) {
               resolve({ "revenueMonth": revenueMonth, "revenueHour": intRevenueHour, "revenueAmount": revenueAmount, "cmiRevenueHour": intCmiRevenueHour, "cmiRevenueAmount": cmiRevenueAmount });
            } else { /* we aren't within SOW timeline */
               resolve({ "revenueMonth": revenueMonth, "revenueHour": 0, "revenueAmount": 0, "cmiRevenueHour": intCmiRevenueHour, "cmiRevenueAmount": cmiRevenueAmount });
            }
         });

         /*commObj.getWeekDaysBetween(calcStartDate, calcStopDate, true, getEmpMonthlyRevenue.name).then((revenueDays) => {
            splWrkObj.getSplWrkWkEndHrs(empEsaLink, strCtsEmpId, cityCode, wrkHrPerDay, strStartDate, strStopDate, getEmpMonthlyRevenue.name).then((weekendWorHours) => {
               let totalWorkHours = (revenueDays * wrkHrPerDay) + weekendWorHours;
               getBufferHours(empJsonObj[2].buffers, monthIndex, wrkHrPerDay).then((bufferHours) => {
                  commObj.calcLeaveHours(empJsonObj[0], strStartDate, strStopDate, getEmpMonthlyRevenue.name).then((effWrkHrs) => {
                     let monthRevenue = 0;
                     let cmiRevenue = 0;
                     let monthRevenueObj = {};
                     let revWorkDays = parseInt(revMonthWorkDays, 10);
                     let locationHolidays = parseInt(locationLeaves, 10);
                     let selfDays = parseInt(personalDays, 10);
                     let buffer = parseInt(bufferDays, 10);
                     let revenueDays = revWorkDays - (locationHolidays + selfDays + buffer);
                     let cmiRevenueDays = revWorkDays - locationHolidays;
                     if (calcStartDate >= sowStart && calcStopDate <= sowEnd) {
                        monthRevenue = revenueDays * wrkHrPerDay * billRatePerHr;
                        cmiRevenue = cmiRevenueDays * wrkHrPerDay * billRatePerHr;
                     }
                     monthRevenueObj = { 'month': dateFormat(dateTime.parse(revenueMonth, "MMYYYY", true), "mmm-yyyy"), 'firstDate': dateFormat(revenueStartDate, "d-mmm-yyyy"), 'lastDate': dateFormat(revenueStopDate, "d-mmm-yyyy"), 'revWorkDays': revWorkDays, 'locationHolidays': locationHolidays, 'selfDays': selfDays, 'bufferDays': buffer, 'monthRevenue': monthRevenue, 'cmiRevenue': cmiRevenue };
                     resolve(monthRevenueObj);
                  }).catch((calcMonthBuffersErr) => { reject(calcMonthBuffersErr); });
               }).catch((countLocationHolidaysErr) => { reject(countLocationHolidaysErr); });
            });
         }).catch((countPersonalDaysErr) => { reject(countPersonalDaysErr); });*/
      }
   });
}


function calcEmpRevenue(empJsonObj, revenueYear, callerName) {
   let monthRevArr = [];
   let funcName = calcEmpRevenue.name;
   return new Promise((resolve, reject) => {
      if (empJsonObj === undefined) {
         reject(funcName + ": Employee object is not provided");
      } else if (revenueYear === undefined) {
         reject(funcName + ": Revenue year is not provided");
      } else if (empJsonObj[0].sowStart === undefined || empJsonObj[0].sowStart == "") {
         reject(funcName + ": SOW start date is not defined for selected employee");
      } else if (empJsonObj[0].sowStop === undefined || empJsonObj[0].sowStop == "") {
         reject(funcName + ": SOW end date is not defined for selected employee");
      } else if (empJsonObj[0].billRatePerHr === undefined || empJsonObj[0].billRatePerHr == "") {
         reject(funcName + ": Billing hour per day is not defined for selected employee");
      } else if (empJsonObj[0].wrkHrPerDay === undefined || empJsonObj[0].wrkHrPerDay == "") {
         reject(funcName + ": Billing rate is not defined for selected employee");
      } else {
         let wrkHrPerDay = parseInt(empJsonObj[0].wrkHrPerDay, 10);
         let billRatePerHr = parseInt(empJsonObj[0].billRatePerHr, 10);
         for (let monthIndex = 0; monthIndex <= 11; monthIndex++) {
            monthRevArr.push(getEmpMonthlyRevenue(empJsonObj, revenueYear, monthIndex, wrkHrPerDay, billRatePerHr, funcName));
         }
      }
      Promise.all(monthRevArr).then((monthRevenue) => {
         logger.end();
         resolve(monthRevenue);
      });
   });
}

module.exports = {
   calcEmpRevenue,
   getEmpMonthlyRevenue
}