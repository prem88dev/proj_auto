const commObj = require("./utility");
const splWrkObj = require("./specialWorkday");
const dateTime = require("date-and-time");
const dateFormat = require("dateformat");


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
         await empBufferArr.forEach((empBuffer) => {
            let bufferMonth = new Date(dateTime.parse(empBuffer.month, "MMM-YYYY", true)).getMonth();
            if (monthIndex === bufferMonth) {
               bufferHours = parseInt(empBuffer.days, 10) * parseInt(wrkHrPerDay, 10);
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
         let intSelfLeaveHour = 0;
         let intLocLeaveHour = 0;
         let intEmpAddlWrkHour = 0;
         let intLocAddlWrkHour = 0

         await computeLeaveHour(empJsonObj[1].leaves, calcStartDate, wrkHrPerDay, funcName).then((selfLeaveHour) => {
            intSelfLeaveHour = parseInt(selfLeaveHour, 10);
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

         if ((dayOfWeek > 0 && dayOfWeek < 6) ||
            ((dayOfWeek === 0 || dayOfWeek === 6) && (intLocAddlWrkHour > 0 || intEmpAddlWrkHour > 0))) {
            if (intSelfLeaveHour > 0 || intLocLeaveHour > 0) {
               let effEmpLeaveHour = intSelfLeaveHour; /* intSelfLeaveHour - 4, intEmpAddlWrkHour - 5 */
               if (intEmpAddlWrkHour <= intSelfLeaveHour) {
                  effEmpLeaveHour = intSelfLeaveHour - intEmpAddlWrkHour;
               } else {
                  effEmpLeaveHour = 0;
               } /* effEmpLeaveHour - 0 */

               let effLocLeaveHour = intLocLeaveHour; /* intLocLeaveHour - 4, intLocAddlWrkHour - 2 */
               if (intLocAddlWrkHour <= intLocLeaveHour) {
                  effLocLeaveHour = intLocLeaveHour - intLocAddlWrkHour;
               } else {
                  effLocLeaveHour = 0;
               } /* effLocLeaveHour - 2 */

               let effLeaveHour = wrkHrPerDay;
               if ((effEmpLeaveHour + effLocLeaveHour) < effLeaveHour) {
                  effLeaveHour -= effEmpLeaveHour + effLocLeaveHour;
               } /* effLeaveHour - 2 */

               if (intEmpAddlWrkHour > 0 && intLocAddlWrkHour > 0) {
                  let effWorkHour = wrkHrPerDay;
                  if ((intEmpAddlWrkHour + intLocAddlWrkHour) < wrkHrPerDay) {
                     effWorkHour = intEmpAddlWrkHour + intLocAddlWrkHour;
                  } /* effWorkHour - 7 */

                  if (effLeaveHour < effWorkHour) {
                     revenueHour += effWorkHour - effLeaveHour;
                  }

                  if (effLocLeaveHour < wrkHrPerDay) {
                     cmiRevenueHour += wrkHrPerDay - effLocLeaveHour;
                  }
               } else if (intLocAddlWrkHour > 0) {
                  if (intSelfLeaveHour < wrkHrPerDay) {
                     revenueHour += wrkHrPerDay - intSelfLeaveHour;
                  }

                  if (effLocLeaveHour <= wrkHrPerDay) {
                     cmiRevenueHour += wrkHrPerDay - effLocLeaveHour;
                  }
               } else if (intEmpAddlWrkHour > 0) {
                  if (effEmpLeaveHour < wrkHrPerDay) {
                     revenueHour += wrkHrPerDay - effEmpLeaveHour;
                  }

                  if (intLocLeaveHour <= wrkHrPerDay) {
                     cmiRevenueHour += wrkHrPerDay - intLocLeaveHour;
                  }
               } else {
                  if (intSelfLeaveHour <= wrkHrPerDay) {
                     revenueHour += wrkHrPerDay - intSelfLeaveHour;
                  }

                  if (intLocLeaveHour <= wrkHrPerDay) {
                     cmiRevenueHour += wrkHrPerDay - intLocLeaveHour;
                  }
               }
            } else if (dayOfWeek > 0 && dayOfWeek < 6) {
               revenueHour += wrkHrPerDay;
               cmiRevenueHour += wrkHrPerDay;
            } else if (dayOfWeek === 0 || dayOfWeek === 6) {
               revenueHour += intEmpAddlWrkHour + intLocAddlWrkHour;
               cmiRevenueHour += intEmpAddlWrkHour + intLocAddlWrkHour;
            }
         }

         calcStartDate.setDate(calcStartDate.getDate() + 1);
         tmpHourArr = { "revenueHour": revenueHour, "cmiRevenueHour": cmiRevenueHour, "nextStartDate": calcStartDate };

         if (calcStartDate.getTime() > calcStopDate.getTime()) {
            return resolve(tmpHourArr)
         } else {
            return resolve(calcRevenueHour(empJsonObj, calcStartDate, stopDate, tmpHourArr, funcName));
         }
      }
   });
}


function getEmpMonthlyRevenue(empJsonObj, revenueYear, monthIndex, callerName) {
   let funcName = getEmpMonthlyRevenue.name;
   return new Promise((resolve, reject) => {
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

         let sowStart = new Date(dateTime.parse(empJsonObj[0].sowStartDate, "D-MMM-YYYY", true));
         sowStart.setHours(0, 0, 0, 0);
         let sowEnd = new Date(dateTime.parse(empJsonObj[0].sowStopDate, "D-MMM-YYYY", true));
         sowEnd.setHours(0, 0, 0, 0);

         let effStopDate = sowEnd;
         if (empJsonObj[0].foreseenStop !== undefined && empJsonObj[0].foreseenStop !== "") {
            let foreseenStop = new Date(dateTime.parse(empJsonObj[0].foreseenStopDate, "D-MMM-YYYY", true));
            foreseenStop.setHours(0, 0, 0, 0);

            if ((foreseenStop.getFullYear() === sowEnd.getFullYear()) && (foreseenStop.getMonth() > sowEnd.getMonth())) {
               effStopDate = foreseenStop;
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

         calcRevenueHour(empJsonObj, calcStartDate, calcStopDate, "", funcName).then((revenueHourArr) => {
            let revenueMonth = dateFormat(calcStartDate, "mmm-yyyy");
            let intRevenueHour = parseInt(revenueHourArr.revenueHour, 10);
            let intCmiRevenueHour = parseInt(revenueHourArr.cmiRevenueHour, 10);
            if (calcStartDate.getTime() >= sowStart.getTime() && calcStopDate.getTime() <= sowEnd.getTime()) {
               resolve({ "revenueMonth": revenueMonth, "revenueHour": intRevenueHour, "cmiRevenueHour": intCmiRevenueHour });
            } else { /* we aren't within SOW timeline */
               resolve({ "revenueMonth": revenueMonth, "revenueHour": 0, "cmiRevenueHour": intCmiRevenueHour });
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
      } else if (empJsonObj[0].sowStartDate === undefined || empJsonObj[0].sowStartDate == "") {
         reject(funcName + ": SOW start date is not defined for selected employee");
      } else if (empJsonObj[0].sowStopDate === undefined || empJsonObj[0].sowStopDate == "") {
         reject(funcName + ": SOW end date is not defined for selected employee");
      } else if (empJsonObj[0].billRatePerHr === undefined || empJsonObj[0].billRatePerHr == "") {
         reject(funcName + ": Billing hour per day is not defined for selected employee");
      } else if (empJsonObj[0].wrkHrPerDay === undefined || empJsonObj[0].wrkHrPerDay == "") {
         reject(funcName + ": Billing rate is not defined for selected employee");
      } else {
         for (let monthIndex = 0; monthIndex <= 11; monthIndex++) {
            monthRevArr.push(getEmpMonthlyRevenue(empJsonObj, revenueYear, monthIndex, funcName));
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