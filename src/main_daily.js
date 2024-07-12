const fs = require('fs');

// Function to determine the time category of a flight based on its actual time
function getTimeCategory(dateTimeString) {
    const date = new Date(dateTimeString);
    const hour = date.getHours();
    const minute = date.getMinutes();

    if (hour === 23 && minute < 30) return "Shoulder hour flights";
    if ((hour === 23 && minute >= 30) || hour < 6) return "Night hour arrivals";
    if (hour === 6) return "Shoulder hour flights";
    return "Regular arrivals";
}

// Function to list flights by day, including the time category and flight number for both arrivals and departures
function listFlightsByDay(flightData) {
    const flightsByDay = {};

    const processFlights = (flights, isArrival = true) => {
        flights.forEach(flight => {
            const flightDate = flight.flight_date;
            const timeCategory = getTimeCategory(isArrival ? flight.arrival.actual : flight.departure.actual);
            const flightNumber = flight.flight.number || 'Unknown';

            if (!flightsByDay[flightDate]) {
                flightsByDay[flightDate] = [];
            }

            flightsByDay[flightDate].push({
                ...flight,
                Time_Category: timeCategory,
                Flight_Number: flightNumber
            });
        });
    };

    // Process both arrivals and departures
    processFlights(flightData.arrivals, true);
    processFlights(flightData.departures, false);

    return flightsByDay;
}

// Function to summarize flight statuses and include time categories for both arrivals and departures
function summarizeFlightStatusesWithTimeCategories(flightData) {
    const statusSummary = {};

    const processFlights = (flights, isArrival = true) => {
        flights.forEach(flight => {
            const status = flight.flight_status;
            const timeCategory = getTimeCategory(isArrival ? flight.arrival.actual : flight.departure.actual);

            if (!statusSummary[status]) {
                statusSummary[status] = { count: 0, timeCategories: {} };
            }
            statusSummary[status].count += 1;

            if (statusSummary[status].timeCategories[timeCategory]) {
                statusSummary[status].timeCategories[timeCategory] += 1;
            } else {
                statusSummary[status].timeCategories[timeCategory] = 1;
            }
        });
    };

    // Process both arrivals and departures
    processFlights(flightData.arrivals, true);
    processFlights(flightData.departures, false);

    return statusSummary;
}

// Path to JSON file
const filePath = '../EGGD_combined_2024-07-11.json'; // Adjust this path to update data

// Read and parse the JSON file
fs.readFile(filePath, 'utf8', (err, data) => {
    if (err) {
        console.error('Error reading the file:', err);
        return;
    }
    try {
        const flightData = JSON.parse(data);

        // Assuming flights are for the same date, get the date from the first flight entry if available
        const flightDate = flightData.arrivals.length > 0 ? flightData.arrivals[0].flight_date :
                            flightData.departures.length > 0 ? flightData.departures[0].flight_date : 'unknown_date';

        // Process and list flights by day for both arrivals and departures
        const flightsByDay = listFlightsByDay(flightData);

        // Summarize flight statuses including time categories for both arrivals and departures
        const flightStatusSummary = summarizeFlightStatusesWithTimeCategories(flightData);
        console.log("Flight Status Summary with Time Categories:", flightStatusSummary);

        // Generate a filename with the flight date
        const outputFilename = `./flights_by_day_${flightDate}.json`;

        // Write the data to a file with the flight date in the filename
        fs.writeFile(outputFilename, JSON.stringify(flightsByDay, null, 2), (err) => {
            if (err) {
                console.error('Error writing to file:', err);
            } else {
                console.log(`Flight data organized by day and written to ${outputFilename} successfully.`);
            }
        });
    } catch (parseErr) {
        console.error('Error parsing JSON:', parseErr);
    }
});
