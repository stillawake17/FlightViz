const fs = require('fs');

// Function to determine the time category of a flight based on its actual arrival time
function getTimeCategory(dateTimeString) {
    const date = new Date(dateTimeString);
    const hour = date.getHours();
    const minute = date.getMinutes();

    if (hour === 23 && minute < 30) return "Shoulder hour flights";
    if ((hour === 23 && minute >= 30) || hour < 6) return "Night hour arrivals";
    if (hour === 6) return "Shoulder hour flights";
    return "Regular arrivals";
}

// Function to list flights by day, including the time category and flight number
function listFlightsByDay(flightData) {
    const flightsByDay = {};

    flightData.arrivals.forEach(flight => {
        const flightDate = flight.flight_date;
        const timeCategory = getTimeCategory(flight.arrival.actual);
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

    return flightsByDay;
}

function summarizeFlightStatuses(flightData) {
    const statusSummary = {};

    // Summarize arrival flight statuses
    flightData.arrivals.forEach(flight => {
        const status = flight.flight_status;
        if (statusSummary[status]) {
            statusSummary[status] += 1;
        } else {
            statusSummary[status] = 1;
        }
    });

    // Summarize departure flight statuses
    flightData.departures.forEach(flight => {
        const status = flight.flight_status;
        if (statusSummary[status]) {
            statusSummary[status] += 1;
        } else {
            statusSummary[status] = 1;
        }
    });

    return statusSummary;
}


// Path to your JSON file
const filePath = '../EGGD_combined_2023-12-05.json'; // Adjust this path as necessary

// Read and parse the JSON file
fs.readFile(filePath, 'utf8', (err, data) => {
    if (err) {
        console.error('Error reading the file:', err);
        return;
    }
    try {
        const flightData = JSON.parse(data);

        // Assuming all flights are for the same date, get the date from the first flight entry
        if (flightData.arrivals.length > 0) {
            const flightDate = flightData.arrivals[0].flight_date;

            // Process and list flights by day
            const flightsByDay = listFlightsByDay(flightData);

            // Summarize flight statuses
            const flightStatusSummary = summarizeFlightStatuses(flightData);
            console.log("Flight Status Summary:", flightStatusSummary);

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
        } else {
            console.log("No flight data found.");
        }

    } catch (parseErr) {
        console.error('Error parsing JSON:', parseErr);
    }
});
