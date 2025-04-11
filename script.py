import pandas as pd
import shelve
import os
import random
import time
import datetime
import hashlib
from tqdm import tqdm
from fast_flights import FlightData, Passengers, Result, get_flights

# Verbose logging function
def log_progress(message, level="INFO"):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{level}] {timestamp}: {message}")

def get_flights_data(origin, destination, date, max_retries=5):
    log_progress(f"Searching Flights: {origin} → {destination}")
    attempt = 0
    while attempt < max_retries:
        try:
            # Create a FlightData object
            flight_data = [
                FlightData(date=date, from_airport=origin, to_airport=destination)
            ]

            # Call the get_flights function with the required parameters
            result: Result = get_flights(
                flight_data=flight_data,
                trip="one-way",
                seat="economy",
                passengers=Passengers(adults=1, children=0, infants_in_seat=0, infants_on_lap=0),
                fetch_mode="fallback",
            )

            # Extract flights from the result
            flights = result.flights
            log_progress(f"Found {len(flights)} flights for {origin} → {destination}")
            return flights
        except Exception as e:
            log_progress(f"Flight Search Failed for {origin} → {destination}: {str(e)}", "WARNING")
            if "no token provided" in str(e).lower():
                attempt += 1
                wait_time = random.randint(1, 5)  # Random wait time between 1 and 5 seconds
                log_progress(f"Retrying ({attempt}/{max_retries}) in {wait_time} seconds...")
                time.sleep(wait_time)  # Wait before retrying
            else:
                # For other exceptions, do not retry and return an empty list
                return []

    log_progress(f"Max retries reached for {origin} → {destination}. Returning no flights.", "ERROR")
    return []


def parse_flight_time(time_str):
    """Parse the flight time string into a datetime object."""
    try:
        # Check if the time string is in ISO 8601 format
        if "T" in time_str:
            # Parse ISO 8601 format
            return datetime.datetime.fromisoformat(time_str)
        else:
            # Parse the human-readable format
            parsed_time = datetime.datetime.strptime(time_str, "%I:%M %p on %a, %b %d")
            # Add the year (since it's not included in the string)
            parsed_time = parsed_time.replace(year=2025)
            return parsed_time
    except ValueError as e:
        log_progress(f"Error parsing time: {time_str} - {str(e)}", "ERROR")
        return None

def is_valid_itinerary(flight_combination, airports, min_city_time_minutes):
    """
    Validate if a given flight combination is a valid itinerary.
    """
    for i in range(len(flight_combination) - 1):
        current_flight = flight_combination[i]
        next_flight = flight_combination[i + 1]

        # Ensure times are pandas.Timestamp objects
        current_arrival = current_flight['arrival']
        next_departure = next_flight['departure']

        # Check if the layover time is sufficient
        layover_time = (next_departure - current_arrival).total_seconds() / 60  # Convert to minutes
        if layover_time < min_city_time_minutes:
            return False

    return True


from itertools import permutations, product

def build_itineraries(df, airports, min_city_time_minutes):
    itineraries = []
    unique_itineraries = set()  # Set to track unique itineraries

    for perm in permutations(airports):
        current_itinerary = []
        valid = True

        for i in range(len(perm) - 1):
            origin = perm[i]
            destination = perm[i + 1]
            possible_flights = df[(df['origin'] == origin) & (df['destination'] == destination)]

            if not possible_flights.empty:
                current_itinerary.append(possible_flights.to_dict('records'))
            else:
                valid = False
                break

        if valid:
            for flight_combination in product(*current_itinerary):
                # Ensure all prices are numeric
                total_price = sum(float(flight['price']) for flight in flight_combination)

                # Create a unique identifier for the itinerary
                itinerary_id = tuple((flight['airline'], flight['origin'], flight['destination'], flight['departure'], flight['arrival']) for flight in flight_combination)

                # Check if this itinerary is already in the set
                if itinerary_id not in unique_itineraries:
                    unique_itineraries.add(itinerary_id)  # Add to the set
                    if is_valid_itinerary(flight_combination, airports, min_city_time_minutes):
                        itineraries.append({
                            "flights": flight_combination,
                            "total_price": total_price
                        })

    log_progress(f"Found {len(itineraries)} valid itineraries")
    return itineraries



def main():
    # Define parameters
    airports = ["NYC", "CHI", "BNA", "AUS"]  # Replace with your 4 airports
    travel_date = "2025-05-11"  # Example start date
    min_city_time_minutes = 90  # Minimum time in each city

    # New constraints
    earliest_departure = "2025-05-11T10:50:00"  # Earliest departure time for the first flight
    latest_arrival = "2025-05-12T00:45:00"  # Latest arrival time for the last flight

    # Progress tracking for flight search
    log_progress("Starting Comprehensive Flight Search")
    itinerary = []

    # Use tqdm for a progress bar
    airport_combinations = [(i, j) for i in airports for j in airports if i != j]
    for origin, destination in tqdm(airport_combinations, desc="Searching Flights"):
        max_retries = 5  # Maximum number of retries
        attempt = 0
        success = False

        while attempt < max_retries and not success:
            try:
                flights = get_cached_flights(origin, destination, travel_date)
                if not flights:
                    log_progress(f"No flights found for {origin} → {destination}", "WARNING")
                    break  # Skip to the next pair if no flights are found

                # Filter for non-stop flights
                non_stop_flights = [flight for flight in flights if flight.stops == 0]  # Keep only non-stop flights

                for flight in non_stop_flights:
                    # Extract flight details
                    price_str = flight.price  # Assuming flight.price is a string like '$54'
                    price = float(price_str.replace('$', '').strip())  # Remove '$' and convert to float
                    departure = parse_flight_time(flight.departure)  # Keep original datetime
                    arrival = parse_flight_time(flight.arrival)  # Keep original datetime

                    # Skip flights with invalid times
                    if not departure or not arrival:
                        continue

                    itinerary.append({
                        "origin": origin,
                        "destination": destination,
                        "departure": departure,  # Store original datetime
                        "arrival": arrival,  # Store original datetime
                        "price": price,  # Ensure price is stored as a float
                        "airline": flight.name  # Use the correct attribute for the airline name
                    })

                success = True  # Set success to True if flights are found
            except Exception as e:
                log_progress(f"Flight Search Failed for {origin} → {destination}: {str(e)}")
                if "no token provided" in str(e):
                    wait_time = random.randint(1, 5)  # Random wait time between 1 and 5 seconds
                    log_progress(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)  # Wait before retrying
                    attempt += 1  # Increment the attempt counter
                else:
                    break  # Break the loop for other errors

    # Convert to DataFrame
    log_progress(f"Total Flights Found: {len(itinerary)}")
    df = pd.DataFrame(itinerary)

    # Check the DataFrame columns
    print("DataFrame Columns:", df.columns)  # Inspect the DataFrame columns

    # Apply earliest departure and latest arrival filters
    log_progress("Applying Earliest Departure and Latest Arrival Constraints")
    if earliest_departure:
        df = df[df["departure"] >= pd.to_datetime(earliest_departure)]  # Convert to datetime for comparison
    if latest_arrival:
        df = df[df["arrival"] <= pd.to_datetime(latest_arrival)]  # Convert to datetime for comparison
    log_progress(f"Flights After Filtering: {len(df)}")

    # Build itinerary
    log_progress("Constructing Optimal Flight Itinerary")
    final_itineraries = build_itineraries(df, airports, min_city_time_minutes)

    # Display results
    log_progress("Final Itinerary Construction Complete")
    if final_itineraries:
        final_itineraries = sorted(final_itineraries, key=lambda x: x["total_price"])
        least_expensive = final_itineraries[0]

        # Pretty print the least expensive itinerary
        print("\n==== LEAST EXPENSIVE ITINERARY ====")
        itinerary_df = pd.DataFrame(least_expensive["flights"])

        # Print header
        print(f"{'Airline':<20}{'Origin':<10}{'Destination':<15}{'Departure':<30}{'Arrival':<30}{'Price':<10}")
        print("=" * 100)

        for index, row in itinerary_df.iterrows():
            # Format departure and arrival for display
            departure_human_readable = row['departure'].strftime("%B %d, %Y, %I:%M %p")
            arrival_human_readable = row['arrival'].strftime("%B %d, %Y, %I:%M %p")
            print(f"{row['airline']:<20}{row['origin']:<10}{row['destination']:<15}{departure_human_readable:<30}{arrival_human_readable:<30}${row['price']:.2f}")

        print(f"\nTotal Price: ${least_expensive['total_price']:.2f}")

        print("\n==== TOP 10 ITINERARIES ====")
        for idx, itinerary in enumerate(final_itineraries[:10], start=1):
            print(f"\nOption {idx}: Total Price = ${itinerary['total_price']:.2f}")
            itinerary_df = pd.DataFrame(itinerary["flights"])

            # Print header
            print(f"{'Airline':<20}{'Origin':<10}{'Destination':<15}{'Departure':<30}{'Arrival':<30}{'Price':<10}")
            print("=" * 100)

            for index, row in itinerary_df.iterrows():
                # Format departure and arrival for display
                departure_human_readable = row['departure'].strftime("%B %d, %Y, %I:%M %p")
                arrival_human_readable = row['arrival'].strftime("%B %d, %Y, %I:%M %p")
                print(f"{row['airline']:<20}{row['origin']:<10}{row['destination']:<15}{departure_human_readable:<30}{arrival_human_readable:<30}${row['price']:.2f}")
    else:
        log_progress("No valid itineraries found.", "WARNING")


def generate_cache_key(origin, destination, date):
    key_string = f"{origin}_{destination}_{date}"
    return hashlib.md5(key_string.encode()).hexdigest()

def get_cached_flights(origin, destination, date, max_cache_age_minutes=1):
    cache_dir = os.path.join(os.path.dirname(__file__), 'flight_cache')
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, 'flight_cache')

    cache_key = generate_cache_key(origin, destination, date)

    try:
        with shelve.open(cache_path) as cache:
            if cache_key in cache:
                cached_data = cache[cache_key]
                current_time = time.time()
                if current_time - cached_data['timestamp'] < (max_cache_age_minutes * 60):
                    log_progress(f"Using cached flights for {origin} → {destination}")
                    return cached_data['flights']
    except Exception as e:
        log_progress(f"Cache access error: {str(e)}", "WARNING")

    log_progress(f"Fetching fresh flights for {origin} → {destination}")
    flights = get_flights_data(origin, destination, date)

    try:
        with shelve.open(cache_path) as cache:
            cache[cache_key] = {
                'flights': flights,
                'timestamp': time.time()
            }
    except Exception as e:
        log_progress(f"Cache storage error: {str(e)}", "WARNING")

    return flights

# Performance and timing
start_time = time.time()
main()
end_time = time.time()

print(f"\nTotal Execution Time: {end_time - start_time:.2f} seconds")
