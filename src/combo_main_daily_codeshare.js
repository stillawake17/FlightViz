const fs = require('fs');

// Function to determine the time category of a flight based on its time
// Modified to use estimated time when actual time is unavailable
function getTimeCategory(dateTimeString) {
    // If dateTimeString is null or undefined, return "Unknown time"
    if (!dateTimeString) {
        return "Unknown time";
    }
    
    const date = new Date(dateTimeString);
    const hour = date.getHours();
    const minute = date.getMinutes();

    if (hour === 23 && minute < 30) return "Shoulder hour flights";
    if ((hour === 23 && minute >= 30) || hour < 6) return "Night hour arrivals";
    if (hour === 6) return "Shoulder hour flights";
    return "Regular arrivals";
}

// Function to get the best available time (actual or estimated if actual is unavailable)
function getBestAvailableTime(timeObj) {
    if (!timeObj) return null;
    return timeObj.actual || timeObj.estimated || null;
}

// Function to list flights by day, including the time category and flight number for both arrivals and departures
function listFlightsByDay(flightData) {
    const flightsByDay = {};
    const processedFlights = new Set(); // Track primary flights to prevent duplicates

    const processFlights = (flights, isArrival = true) => {
        flights.forEach(flight => {
            // Check for codeshare and skip if the primary flight is already processed
            const codeshared = flight.flight?.codeshared;
            if (codeshared && processedFlights.has(codeshared.flight_number)) {
                return; // Skip this codeshare flight as the primary has already been processed
            }

            const flightDate = flight.flight_date;
            
            // Use getBestAvailableTime to get the most accurate time information
            const timeObj = isArrival ? flight.arrival : flight.departure;
            const bestTime = getBestAvailableTime(timeObj);
            const timeCategory = getTimeCategory(bestTime);
            
            const flightNumber = flight.flight?.number || 'Unknown';

            // Debug print: Log flight details, time category, and whether it's a codeshare
            console.log(`Flight: ${flightNumber}, Time Category: ${timeCategory}, Is Codeshare: ${!!codeshared}, Time Used: ${bestTime || 'N/A'}`);

            // Initialize date entry if it doesn't exist
            if (!flightsByDay[flightDate]) {
                flightsByDay[flightDate] = [];
            }

            flightsByDay[flightDate].push({
                ...flight,
                Time_Category: timeCategory,
                Flight_Number: flightNumber,
                Time_Used: bestTime || 'N/A'
            });

            // Track the primary flight number to avoid counting codeshare duplicates
            processedFlights.add(flightNumber);
        });
    };

    // Process both arrivals and departures
    if (flightData.arrivals && Array.isArray(flightData.arrivals)) {
        processFlights(flightData.arrivals, true);
    }
    
    if (flightData.departures && Array.isArray(flightData.departures)) {
        processFlights(flightData.departures, false);
    }

    return flightsByDay;
}

// Function to summarize flight statuses and include time categories for both arrivals and departures
function summarizeFlightStatusesWithTimeCategories(flightData) {
    const statusSummary = {};
    const processedFlights = new Set(); // Track primary flights to prevent duplicate counting

    const processFlights = (flights, isArrival = true) => {
        flights.forEach(flight => {
            // Check for codeshare and skip if the primary flight is already processed
            const codeshared = flight.flight?.codeshared;
            if (codeshared && processedFlights.has(codeshared.flight_number)) {
                return; // Skip this codeshare flight as the primary has already been processed
            }

            const status = flight.flight_status;
            
            // Use getBestAvailableTime to get the most accurate time information
            const timeObj = isArrival ? flight.arrival : flight.departure;
            const bestTime = getBestAvailableTime(timeObj);
            const timeCategory = getTimeCategory(bestTime);

            // Initialize status entry if it doesn't exist
            if (!statusSummary[status]) {
                statusSummary[status] = { count: 0, timeCategories: {} };
            }
            statusSummary[status].count += 1;

            // Increment time category count
            if (statusSummary[status].timeCategories[timeCategory]) {
                statusSummary[status].timeCategories[timeCategory] += 1;
            } else {
                statusSummary[status].timeCategories[timeCategory] = 1;
            }

            // Track the primary flight number to avoid counting codeshare duplicates
            if (flight.flight?.number) {
                processedFlights.add(flight.flight.number);
            }
        });
    };

    // Process both arrivals and departures
    if (flightData.arrivals && Array.isArray(flightData.arrivals)) {
        processFlights(flightData.arrivals, true);
    }
    
    if (flightData.departures && Array.isArray(flightData.departures)) {
        processFlights(flightData.departures, false);
    }

    return statusSummary;
}

// Path to JSON file - using command line argument if provided, or default path
const filePath = process.argv[2] || '../EGGD_combined_2024-12-01.json';
console.log(`Using file path: ${filePath}`);

// Read and parse the JSON file
fs.readFile(filePath, 'utf8', (err, data) => {
    if (err) {
        console.error('Error reading the file:', err);
        return;
    }
    try {
        const flightData = JSON.parse(data);

        // Validate the structure of the data
        if (!flightData.arrivals || !flightData.departures) {
            console.error('Invalid data format: missing arrivals or departures arrays');
            return;
        }

        // Getting date info from data
        const flightDate = flightData.arrivals.length > 0 ? flightData.arrivals[0].flight_date :
                          flightData.departures.length > 0 ? flightData.departures[0].flight_date : 'unknown_date';

        console.log(`Processing flights for date: ${flightDate}`);
        console.log(`Total arrivals: ${flightData.arrivals.length}`);
        console.log(`Total departures: ${flightData.departures.length}`);

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