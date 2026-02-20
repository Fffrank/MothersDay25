#!/home/rbosworth/Projects/MothersDay25/.venv/bin/python
import pandas as pd
import shelve
import os
import random
import time
import datetime
import hashlib
import io
import contextlib
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
            # Note: fast-flights v2.2 supports "common", "fallback", "force-fallback"
            # "local" mode is only available in v3.0+ - v2.2 handles HTTP requests internally
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                result: Result = get_flights(
                    flight_data=flight_data,
                    trip="one-way",
                    seat="economy",
                    passengers=Passengers(adults=1, children=0, infants_in_seat=0, infants_on_lap=0),
                    fetch_mode="common",
                )

            # Extract flights from the result
            flights = result.flights
            log_progress(f"Found {len(flights)} flights for {origin} → {destination}")
            return flights
        except Exception as e:
            log_progress(f"Flight Search Failed for {origin} → {destination}: {str(e)}", "WARNING")
            error_str = str(e).lower()
            # Handle different types of errors
            if "no token provided" in error_str or "timeout" in error_str or "no flights found" in error_str:
                attempt += 1
                if attempt < max_retries:
                    wait_time = random.randint(2, 8)  # Random wait time between 2 and 8 seconds
                    log_progress(f"Retrying ({attempt}/{max_retries}) in {wait_time} seconds...")
                    time.sleep(wait_time)  # Wait before retrying
                else:
                    log_progress(f"Max retries reached for {origin} → {destination}. Returning no flights.", "ERROR")
                    return []
            else:
                # For other exceptions, do not retry and return an empty list
                return []

    log_progress(f"Max retries reached for {origin} → {destination}. Returning no flights.", "ERROR")
    return []


def parse_flight_time(time_str, travel_date_str):
    """Parse the flight time string into a datetime object.
    
    Args:
        time_str: Time string from fast_flights (e.g., "2:30 PM on Mon, May 10" or ISO format)
        travel_date_str: Travel date string in YYYY-MM-DD format to extract the year
    """
    try:
        # Check if the time string is in ISO 8601 format
        if "T" in time_str:
            # Parse ISO 8601 format
            return datetime.datetime.fromisoformat(time_str)
        else:
            # Parse the human-readable format (e.g., "2:30 PM on Mon, May 10")
            # Try different date formats that Google Flights might use
            year = datetime.datetime.strptime(travel_date_str, "%Y-%m-%d").year
            
            # Try format with "on" (e.g., "2:30 PM on Mon, May 10")
            try:
                parsed_time = datetime.datetime.strptime(time_str, "%I:%M %p on %a, %b %d")
                parsed_time = parsed_time.replace(year=year)
                return parsed_time
            except ValueError:
                pass
            
            # Try format without "on" (e.g., "2:30 PM Mon, May 10")
            try:
                parsed_time = datetime.datetime.strptime(time_str, "%I:%M %p %a, %b %d")
                parsed_time = parsed_time.replace(year=year)
                return parsed_time
            except ValueError:
                pass
            
            # Try just time (e.g., "2:30 PM") - use travel date
            try:
                parsed_time = datetime.datetime.strptime(time_str, "%I:%M %p")
                travel_date = datetime.datetime.strptime(travel_date_str, "%Y-%m-%d")
                parsed_time = parsed_time.replace(year=travel_date.year, month=travel_date.month, day=travel_date.day)
                return parsed_time
            except ValueError:
                pass
            
            # If all parsing attempts fail, log and return None
            log_progress(f"Could not parse time format: {time_str}", "WARNING")
            return None
    except Exception as e:
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



def prompt_airports():
    all_airports = ["NYC", "AUS", "CHI", "BNA", "CHS"]

    while True:
        try:
            num = int(input("How many cities would you like to search? (3-5): ").strip())
            if 3 <= num <= 5:
                break
            print("Please enter a number between 3 and 5.")
        except ValueError:
            print("Please enter a valid number.")

    if num == 5:
        print(f"Using all 5 cities: {', '.join(all_airports)}")
        return all_airports

    print(f"\nAvailable airports:")
    for i, airport in enumerate(all_airports, start=1):
        print(f"  {i}. {airport}")

    while True:
        try:
            raw = input(f"Select {num} airports by number, separated by spaces (e.g. 1 3 4): ").strip()
            parts = raw.split()
            if len(parts) != num:
                print(f"Please select exactly {num} airports.")
                continue
            indices = [int(p) - 1 for p in parts]
            if len(set(indices)) != num:
                print("Please select distinct airports.")
                continue
            if any(i < 0 or i >= len(all_airports) for i in indices):
                print(f"Please enter numbers between 1 and {len(all_airports)}.")
                continue
            selected = [all_airports[i] for i in indices]
            print(f"Selected: {', '.join(selected)}")
            return selected
        except ValueError:
            print("Please enter valid numbers.")


def main():
    # Define parameters
    airports = prompt_airports()
    travel_date = "2026-05-10"  # Example start date
    min_city_time_minutes = 90  # Minimum time in each city

    # New constraints
    earliest_departure = "2026-05-10T10:50:00"  # Earliest departure time for the first flight
    latest_arrival = "2026-05-11T00:45:00"  # Latest arrival time for the last flight

    # Progress tracking for flight search
    log_progress("Starting Comprehensive Flight Search")
    itinerary = []
    zero_flight_routes = []

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
                    zero_flight_routes.append((origin, destination))
                    break  # Skip to the next pair if no flights are found

                # Filter for non-stop flights
                non_stop_flights = [flight for flight in flights if flight.stops == 0]  # Keep only non-stop flights

                for flight in non_stop_flights:
                    # Extract flight details
                    price_str = flight.price  # Assuming flight.price is a string like '$54'
                    price = float(price_str.replace('$', '').strip())  # Remove '$' and convert to float
                    
                    # Debug: log first flight's time format to understand the structure
                    if len(itinerary) == 0:
                        log_progress(f"Sample flight time formats - departure: '{flight.departure}', arrival: '{flight.arrival}'")
                    
                    departure = parse_flight_time(flight.departure, travel_date)  # Parse with travel date for year
                    arrival = parse_flight_time(flight.arrival, travel_date)  # Parse with travel date for year

                    # Skip flights with invalid times
                    if not departure or not arrival:
                        if len(itinerary) == 0:  # Only log for first flight to avoid spam
                            log_progress(f"Skipping flight due to parsing failure - departure: {flight.departure}, arrival: {flight.arrival}", "WARNING")
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
    if zero_flight_routes:
        log_progress(f"{len(zero_flight_routes)} route(s) returned no flights:", "WARNING")
        for orig, dest in zero_flight_routes:
            log_progress(f"  No flights found: {orig} → {dest}", "WARNING")
    
    if not itinerary:
        log_progress("No flights found. Cannot build itineraries.", "WARNING")
        return
    
    df = pd.DataFrame(itinerary)

    # Check the DataFrame columns
    print("DataFrame Columns:", df.columns)  # Inspect the DataFrame columns

    # Apply earliest departure and latest arrival filters
    log_progress("Applying Earliest Departure and Latest Arrival Constraints")
    if not df.empty:
        earliest_dt = pd.to_datetime(earliest_departure) if earliest_departure else None
        latest_dt = pd.to_datetime(latest_arrival) if latest_arrival else None
        
        log_progress(f"Filtering with earliest_departure: {earliest_dt}, latest_arrival: {latest_dt}")
        
        if earliest_departure:
            before_count = len(df)
            df = df[df["departure"] >= earliest_dt]
            log_progress(f"After earliest_departure filter: {len(df)} flights (removed {before_count - len(df)})")
            if len(df) > 0:
                log_progress(f"Sample departure times after filter: {df['departure'].head(3).tolist()}")
        
        if latest_arrival and not df.empty:
            before_count = len(df)
            df = df[df["arrival"] <= latest_dt]
            log_progress(f"After latest_arrival filter: {len(df)} flights (removed {before_count - len(df)})")
    log_progress(f"Final flights after all filtering: {len(df)}")

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

    if flights:  # Don't cache empty results so failed routes are retried next run
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
if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    
    print(f"\nTotal Execution Time: {end_time - start_time:.2f} seconds")
