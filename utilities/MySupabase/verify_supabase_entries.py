import os
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables from .env file
load_dotenv()

# Get Supabase credentials from environment variables
SUPABASE_URL: str = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

# Initialize Supabase client using the service role key for full access
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def verify_entries():
    """Fetches and prints all entries from the public.entry_by_agent table."""
    try:
        response = supabase.table("entry_by_agent").select("*", count='exact').execute()
        data = response.data

        if data:
            print("\n--- Entries in public.entry_by_agent ---")
            for entry in data:
                print(entry)
            print("----------------------------------------")
        else:
            print("No entries found in public.entry_by_agent.")

    except Exception as e:
        print(f"An error occurred during verification: {e}")
        print("Please ensure your Supabase URL and SERVICE_ROLE_KEY are correctly set in the .env file.")

if __name__ == "__main__":
    verify_entries()
