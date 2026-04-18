import requests
import time
import collections
from multiprocessing import Pool, cpu_count

# Configuration
BASE_URL = "http://72.60.221.150:8080"
STUDENT_ID = "MDS202508"  # Replace with your actual student ID

NUM_WORKERS = min(cpu_count(), 8)  # Limit workers to avoid throttling
CHUNK_SIZE = 1000 // NUM_WORKERS


def login(student_id):
    """Log in to get a dynamic SHA256 secret key."""
    response = requests.post(
        f"{BASE_URL}/login",
        json={"student_id": student_id}
    )
    response.raise_for_status()
    return response.json()["secret_key"]


def get_publication_title(student_id, filename):
    """
    1. Log in to get a dynamic SHA256 secret key.
    2. Use the key to retrieve the publication title for the given file.
    3. Handle 429 (Too Many Requests) by implementing a retry mechanism.
    4. Handle other errors (404, 500, etc.) appropriately.
    """
    secret_key = login(student_id)

    max_retries = 5
    backoff = 1  # seconds

    for attempt in range(max_retries):
        try:
            response = requests.post(
                f"{BASE_URL}/lookup",
                json={"secret_key": secret_key, "filename": filename}
            )
            if response.status_code == 429:
                print(f"[429] Rate limited on {filename}. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
                continue
            elif response.status_code == 404:
                print(f"[404] File not found: {filename}. Skipping.")
                return None
            elif response.status_code == 500:
                print(f"[500] Server error on {filename}. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
                continue
            else:
                response.raise_for_status()
                return response.json().get("title", None)
        except requests.RequestException as e:
            print(f"[ERROR] {filename} attempt {attempt + 1}: {e}")
            time.sleep(backoff)
            backoff *= 2

    print(f"[FAILED] Could not retrieve {filename} after {max_retries} attempts.")
    return None


def mapper(filename_chunk):
    """
    Map phase: Takes a chunk (list) of filenames, retrieves each title,
    extracts the first word, and returns a Counter of first-word frequencies.
    """
    counter = collections.Counter()

    for filename in filename_chunk:
        title = get_publication_title(STUDENT_ID, filename)
        if title:
            first_word = title.strip().split()[0] if title.strip() else None
            if first_word:
                counter[first_word] += 1
        # Small sleep to respect the 100 req/sec throttle across workers
        time.sleep(0.02)

    return counter


def reducer(counters):
    """
    Reduce phase: Merges all Counter objects from mapper workers into one.
    """
    total = collections.Counter()
    for c in counters:
        total.update(c)
    return total


def verify_top_10(student_id, top_10_list):
    """
    1. Log in to get a dynamic SHA256 secret key.
    2. Submit the top_10_list to the /verify endpoint.
    3. Print the final score and message from the server.
    """
    secret_key = login(student_id)

    response = requests.post(
        f"{BASE_URL}/verify",
        json={"secret_key": secret_key, "top_10": top_10_list}
    )
    response.raise_for_status()
    result = response.json()

    print("\n========== VERIFICATION RESULT ==========")
    print(f"Score   : {result.get('score')} / {result.get('total')}")
    print(f"Correct : {result.get('correct')}")
    print(f"Message : {result.get('message')}")
    print("=========================================\n")

    return result


if __name__ == "__main__":
    # Step 1: Build the full list of filenames
    all_filenames = [f"pub_{i}.txt" for i in range(1000)]

    # Step 2: Divide into chunks for parallel processing
    chunks = [
        all_filenames[i:i + CHUNK_SIZE]
        for i in range(0, len(all_filenames), CHUNK_SIZE)
    ]
    print(f"Using {NUM_WORKERS} workers with ~{CHUNK_SIZE} files each.")

    # Step 3: Map phase — parallel title retrieval and first-word counting
    print("Starting Map phase...")
    with Pool(processes=NUM_WORKERS) as pool:
        results = pool.map(mapper, chunks)

    # Step 4: Reduce phase — merge all counters
    print("Starting Reduce phase...")
    combined = reducer(results)

    # Step 5: Extract Top 10 most frequent first words
    top_10 = [word for word, count in combined.most_common(10)]

    print("\n========== TOP 10 FIRST WORDS ==========")
    for rank, (word, count) in enumerate(combined.most_common(10), start=1):
        print(f"  {rank:2}. {word:<20} ({count} occurrences)")
    print("=========================================\n")

    # Step 6: Verify with the server
    if top_10:
        verify_top_10(STUDENT_ID, top_10)
    else:
        print("Could not compute top 10 words. Check your connection and student ID.")
