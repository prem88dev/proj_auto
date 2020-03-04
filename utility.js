/* calculate number of days between */
function getDaysBetween(startDate, endDate, getWeekDays) {
   let daysBetween = 0;
   return new Promise((resolve, _reject) => {
      if (startDate === undefined || endDate === undefined) {
         resolve(daysBetween);
      } else {
         /* clone date to avoid messing up original data */
         let fromDate = new Date(startDate);
         let toDate = new Date(endDate);

         fromDate.setUTCHours(0, 0, 0, 0);
         endDate.setUTCHours(0, 0, 0, 0);

         if (fromDate.getTime() === toDate.getTime()) {
            let dayOfWeek = fromDate.getDay();
            if (getWeekDays === true) {
               if (dayOfWeek > 0 && dayOfWeek < 6) {
                  daysBetween++;
               }
            } else {
               daysBetween++;
            }
            resolve(daysBetween);
         } else {
            while (fromDate <= toDate) {
               fromDate.setDate(fromDate.getDate() + 1);
               let dayOfWeek = fromDate.getDay();
               /* check if the date is neither a Sunday(0) nor a Saturday(6) */
               if (getWeekDays === true) {
                  if (dayOfWeek > 0 && dayOfWeek < 6) {
                     daysBetween++;
                  }
               } else {
                  daysBetween++;
               }
            }
            resolve(daysBetween);
         }
      }
   });
};

function computeWeekdaysInLeave(leaveArr) {
   let weekdaysInLeave = 0;
   return new Promise(async (resolve, _reject) => {
      await leaveArr.forEach((leave) => {
         getDaysBetween(leave.startDate, leave.endDate, true).then((weekdays) => {
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
         getDaysBetween(leave.startDate, leave.endDate, false).then((daysBetween) => {
            leaveDays += daysBetween;
         });
      });
      resolve(leaveDays);
   });
}

function computeBufferDays(bufferArr) {
   let bufferDays = 0;
   return new Promise(async (resolve, _reject) => {
      await bufferArr.forEach((buffer) => {
         bufferDays += parseInt(buffer.days, 10);
      });
      resolve(bufferDays);
   });
}


module.exports = {
   getDaysBetween,
   computeWeekdaysInLeave,
   computeLeaveDays,
   computeBufferDays
}