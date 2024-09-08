import subprocess
import time

def postgres_runs_first(host, max_tries=5, delay=5):
    tries = 0
    while tries < max_tries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host],
                check=True,
                capture_output=True,
                text=True
            )
            if "accepting connections" in result.stdout:
                print("Connected to PosgreSQL!")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to PostgreSQL: {e}")
            tries += 1
            print(f"Retrying in {delay} seconds... (Attempt {tries}/{max_tries})")
            time.sleep(delay)
    print("Maximum tries reached. Exiting.")
    return False