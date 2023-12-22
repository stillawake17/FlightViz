// Function to aggregate flight data by month for a given year
function aggregateDataByMonth(flightData, year) {
    const monthlyCounts = new Array(12).fill(0); // Array for each month
    
    flightData.forEach(d => {
        // Assuming the date is in d.arrival.actualTime or d.departure.actualTime
        let actualTime = d.arrival ? d.arrival.actualTime : (d.departure ? d.departure.actualTime : null);
        
        if (actualTime) {
            let date = new Date(actualTime);
            let flightYear = date.getFullYear();
            let flightMonth = date.getMonth(); // getMonth() returns 0-11 for Jan-Dec
            
            if (flightYear === year) {
                monthlyCounts[flightMonth]++;
            }
        }
    });

    return monthlyCounts;
}


//TO TEST
// Example usage with mock data
// let mockFlightData = [
//     { arrival: { actualTime: "2023-01-01T18:57:00.000" } },
//     { departure: { actualTime: "2023-02-15T11:30:00.000" } },
//     // ... more flight data
// ];

// console.log(aggregateDataByMonth(mockFlightData, 2023));
