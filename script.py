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
from fast_flights import FlightQuery, Passengers, create_query, get_flights

# Verbose logging function
def log_progress(message, level="INFO"):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{level}] {timestamp}: {message}")

def get_flights_data(origin, destination, date, max_retries=5):
    log_progress(f"Searching Flights: {origin} → {destination}")
    attempt = 0
    while attempt < max_retries:
        try:
            query = create_query(
                flights=[FlightQuery(date=date, from_airport=origin, to_airport=destination, max_stops=0)],
                trip="one-way",
                seat="economy",
                passengers=Passengers(adults=1),
                language="en",
            )
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                result = get_flights(query)
            log_progress(f"Found {len(result)} flights for {origin} → {destination}")
            return result
        except Exception as e:
            error_summary = str(e).split('\n')[0][:150]
            log_progress(f"Flight Search Failed for {origin} → {destination}: {error_summary}", "WARNING")
            attempt += 1
            if attempt < max_retries:
                wait_time = random.randint(2, 8)
                log_progress(f"Retrying ({attempt}/{max_retries}) in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                log_progress(f"Max retries reached for {origin} → {destination}. Returning no flights.", "ERROR")
                return []
    return []

def is_valid_itinerary(flight_combination, airports, min_city_time_minutes, min_nyc_time_minutes=None, earliest_departure=None, latest_arrival=None):
    """
    Validate if a given flight combination is a valid itinerary.
    earliest_departure applies only to the first flight's departure.
    latest_arrival applies only to the last flight's arrival.
    NYC layovers use min_nyc_time_minutes when provided.
    """
    if earliest_departure and flight_combination[0]['departure'] < earliest_departure:
        return False
    if latest_arrival and flight_combination[-1]['arrival'] > latest_arrival:
        return False

    for i in range(len(flight_combination) - 1):
        current_flight = flight_combination[i]
        next_flight = flight_combination[i + 1]

        current_arrival = current_flight['arrival']
        next_departure = next_flight['departure']

        layover_time = (next_departure - current_arrival).total_seconds() / 60
        min_time = (
            min_nyc_time_minutes
            if min_nyc_time_minutes and current_flight['destination'] == 'NYC'
            else min_city_time_minutes
        )
        if layover_time < min_time:
            return False

    return True


from itertools import combinations, permutations, product

def effective_price(flight, companion_pass):
    price = float(flight['price'])
    if companion_pass and "southwest" in flight['airline'].lower():
        return price / 2
    return price

def build_itineraries(df, airports, num_cities, min_city_time_minutes, min_nyc_time_minutes=None, earliest_departure=None, latest_arrival=None, companion_pass=False, require_chs=False):
    itineraries = []
    unique_itineraries = set()

    for combo in combinations(airports, num_cities):
        if require_chs and "CHS" not in combo:
            continue
        for perm in permutations(combo):
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
                    total_price = sum(effective_price(f, companion_pass) for f in flight_combination)

                    itinerary_id = tuple((flight['airline'], flight['origin'], flight['destination'], flight['departure'], flight['arrival']) for flight in flight_combination)

                    if itinerary_id not in unique_itineraries:
                        unique_itineraries.add(itinerary_id)
                        if is_valid_itinerary(flight_combination, airports, min_city_time_minutes, min_nyc_time_minutes, earliest_departure, latest_arrival):
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
        return all_airports, 5

    # For 3 or 4 cities, ask whether to find the best N from all cities or pick specific ones
    while True:
        choice = input(f"Find best {num} from all cities, or pick specific ones? (any/pick): ").strip().lower()
        if choice in ("any", "pick"):
            break
        print("  Please enter 'any' or 'pick'.")

    if choice == "any":
        print(f"Will search all cities and find the best {num}-city itinerary.")
        return all_airports, num

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
            return selected, num
        except ValueError:
            print("Please enter valid numbers.")


def prompt_time(label, default_time, date_str):
    """Prompt for HH:MM, combining with date_str to return a full ISO datetime string."""
    while True:
        raw = input(f"{label} [{default_time}]: ").strip()
        if not raw:
            raw = default_time
        try:
            datetime.datetime.strptime(raw, "%H:%M")
            return f"{date_str}T{raw}:00"
        except ValueError:
            print("  Invalid format. Use HH:MM (e.g. 08:30)")


def prompt_int(label, default, min_val=1):
    """Prompt for a positive integer, showing the default."""
    while True:
        raw = input(f"{label} [{default}]: ").strip()
        if not raw:
            return default
        try:
            val = int(raw)
            if val >= min_val:
                return val
            print(f"  Please enter a value >= {min_val}.")
        except ValueError:
            print("  Please enter a valid integer.")


def prompt_constraints():
    print("\n-- Flight Constraints --")
    print("Press Enter to accept defaults.\n")
    earliest_departure = prompt_time(
        "Earliest departure time on May 10 (HH:MM)",
        "10:50", "2026-05-10"
    )
    latest_arrival = prompt_time(
        "Latest arrival time on May 11   (HH:MM)",
        "00:45", "2026-05-11"
    )
    min_city_time_minutes = prompt_int(
        "Minimum stopover time in all cities (minutes)",
        90, min_val=0
    )
    min_nyc_time_minutes = prompt_int(
        "Minimum stopover time in NYC (minutes)",
        180, min_val=0
    )
    while True:
        raw = input("Do you have a Southwest Companion Pass? (y/n): ").strip().lower()
        if raw in ("y", "n"):
            companion_pass = raw == "y"
            break
        print("  Please enter 'y' or 'n'.")
    while True:
        raw = input("Is CHS a required city? (y/n): ").strip().lower()
        if raw in ("y", "n"):
            require_chs = raw == "y"
            break
        print("  Please enter 'y' or 'n'.")
    return earliest_departure, latest_arrival, min_city_time_minutes, min_nyc_time_minutes, companion_pass, require_chs


def main():
    airports, num_cities = prompt_airports()
    earliest_departure, latest_arrival, min_city_time_minutes, min_nyc_time_minutes, companion_pass, require_chs = prompt_constraints()
    travel_date = earliest_departure[:10]  # derive date from earliest departure

    # Progress tracking for flight search
    log_progress("Starting Comprehensive Flight Search")
    log_progress(f"Travel date: {travel_date}, window: {earliest_departure} → {latest_arrival}, min stopover: {min_city_time_minutes}m (NYC: {min_nyc_time_minutes}m)")
    itinerary = []
    zero_flight_routes = []   # API returned nothing

    # Use tqdm for a progress bar
    airport_combinations = [(i, j) for i in airports for j in airports if i != j]
    for origin, destination in tqdm(airport_combinations, desc="Searching Flights"):
        flights = get_cached_flights(origin, destination, travel_date)
        if not flights:
            log_progress(f"No flights found for {origin} → {destination}", "WARNING")
            zero_flight_routes.append((origin, destination))
            continue

        for f in flights:
            leg = f.flights[0]  # single non-stop leg
            dep = leg.departure
            arr = leg.arrival
            departure = datetime.datetime(dep.date[0], dep.date[1], dep.date[2], dep.hour, dep.minute)
            arrival = datetime.datetime(arr.date[0], arr.date[1], arr.date[2], arr.hour, arr.minute)
            itinerary.append({
                "origin": origin,
                "destination": destination,
                "departure": departure,
                "arrival": arrival,
                "price": float(f.price),
                "airline": ", ".join(f.airlines) if f.airlines else "Unknown",
            })

    # Convert to DataFrame
    log_progress(f"Total Flights Found: {len(itinerary)}")
    if zero_flight_routes:
        log_progress(f"{len(zero_flight_routes)} route(s) returned NO flights from API: {', '.join(f'{o}→{d}' for o,d in zero_flight_routes)}", "WARNING")

    if not itinerary:
        log_progress("No flights found. Cannot build itineraries.", "WARNING")
        return

    df = pd.DataFrame(itinerary)

    earliest_dt = pd.to_datetime(earliest_departure) if earliest_departure else None
    latest_dt = pd.to_datetime(latest_arrival) if latest_arrival else None
    log_progress(f"Itinerary constraints: first departure >= {earliest_dt}, last arrival <= {latest_dt}")
    log_progress(f"Total flights available for itinerary search: {len(df)}")

    # Log per-route coverage so we can diagnose missing legs
    empty_routes = []
    for orig in airports:
        for dest in airports:
            if orig != dest:
                count = len(df[(df['origin'] == orig) & (df['destination'] == dest)])
                if count == 0:
                    empty_routes.append(f"{orig} → {dest}")
    if empty_routes:
        log_progress(f"Routes with NO usable flights (will block itineraries): {', '.join(empty_routes)}", "WARNING")

    # Build itinerary
    log_progress("Constructing Optimal Flight Itinerary")
    if companion_pass:
        log_progress("Companion Pass active: Southwest prices halved in calculations")
    final_itineraries = build_itineraries(df, airports, num_cities, min_city_time_minutes, min_nyc_time_minutes, earliest_dt, latest_dt, companion_pass, require_chs)

    # Display results
    log_progress("Final Itinerary Construction Complete")
    if final_itineraries:
        final_itineraries = sorted(final_itineraries, key=lambda x: x["total_price"])
        least_expensive = final_itineraries[0]

        cp_note = "  * Southwest prices halved (Companion Pass)" if companion_pass else ""

        def print_itinerary_rows(flights_df):
            for _, row in flights_df.iterrows():
                dep = row['departure'].strftime("%B %d, %Y, %I:%M %p")
                arr = row['arrival'].strftime("%B %d, %Y, %I:%M %p")
                price = effective_price(row, companion_pass)
                cp_tag = " (CP)" if companion_pass and "southwest" in row['airline'].lower() else ""
                print(f"{row['airline']:<20}{row['origin']:<10}{row['destination']:<15}{dep:<30}{arr:<30}${price:.2f}{cp_tag}")

        # Pretty print the least expensive itinerary
        print(f"\n==== LEAST EXPENSIVE ITINERARY ===={cp_note}")
        itinerary_df = pd.DataFrame(least_expensive["flights"])
        print(f"{'Airline':<20}{'Origin':<10}{'Destination':<15}{'Departure':<30}{'Arrival':<30}{'Price':<10}")
        print("=" * 100)
        print_itinerary_rows(itinerary_df)
        print(f"\nTotal Price: ${least_expensive['total_price']:.2f}")

        print("\n==== TOP 10 ITINERARIES ====")
        for idx, itinerary in enumerate(final_itineraries[:10], start=1):
            print(f"\nOption {idx}: Total Price = ${itinerary['total_price']:.2f}")
            itinerary_df = pd.DataFrame(itinerary["flights"])
            print(f"{'Airline':<20}{'Origin':<10}{'Destination':<15}{'Departure':<30}{'Arrival':<30}{'Price':<10}")
            print("=" * 100)
            print_itinerary_rows(itinerary_df)
    else:
        log_progress("No valid itineraries found.", "WARNING")
        window_h = (pd.to_datetime(latest_arrival) - pd.to_datetime(earliest_departure)).total_seconds() / 3600
        log_progress(f"Tip: {num_cities} cities × {min_city_time_minutes}min min stopover in a "
                     f"{window_h:.1f}h window may be too tight. "
                     f"Try fewer cities, a wider time window, or a shorter min stopover.", "WARNING")


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
