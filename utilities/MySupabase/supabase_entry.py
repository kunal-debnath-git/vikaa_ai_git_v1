import os
import random
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables from .env file
load_dotenv()

# Get Supabase credentials from environment variables
SUPABASE_URL: str = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY: str = os.environ.get("SUPABASE_ANON_KEY")
SUPABASE_SERVICE_ROLE_KEY: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") # Not strictly needed for RLS-enabled public inserts, but good to have for other operations

# Initialize Supabase client
# Using anon key for public table insert, assuming RLS allows it.
# If RLS prevents insert with anon key, you might need to use the service role key:
# supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def insert_agent_entry():
    """Inserts a new entry into the public.entry_by_agent table."""
    try:
        random_number = random.randint(1000, 9999) # Generate a 4-digit random number
        agent_text_value = f"yesNEW_{random_number}"

        data, count = supabase.table("entry_by_agent").insert({"agent_text": agent_text_value}).execute()

        if data:
            print(f"Successfully inserted entry: {data[1]}")
        else:
            print("No data returned from insert operation.")

    except Exception as e:
        print(f"An error occurred: {e}")
        print("Please ensure your Supabase URL and API keys are correctly set in the .env file.")
        print("Also, check your Row Level Security (RLS) policies on the 'entry_by_agent' table.")

if __name__ == "__main__":
    insert_agent_entry()
