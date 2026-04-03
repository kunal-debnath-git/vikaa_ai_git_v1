# MCP Server Daily Entry Setup

This document provides instructions on how to set up a daily automated entry into your Supabase `entry_by_agent` table, simulating a daily check-in for a Management Control Plane (MCP) server. This setup uses Windows Task Scheduler to run a Python script once every day.

## Prerequisites

Before proceeding, ensure you have:

1.  The `supabase_entry.py` script located in `E:\_Gemini_CLI_TRY\MySupabase`.
2.  Your `.env` file correctly configured with `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` in the project root (`E:\_Gemini_CLI_TRY\.env`).
3.  Python and the required libraries (`python-dotenv`, `supabase`) installed in your environment.

## Setup using Windows Task Scheduler

Follow these steps to schedule the `supabase_entry.py` script to run daily:

1.  **Open Task Scheduler:**
    *   Press `Windows Key + R`, type `taskschd.msc`, and press Enter.
    *   Alternatively, search for "Task Scheduler" in the Start Menu.

2.  **Create a Basic Task:**
    *   In the Task Scheduler window, in the right-hand "Actions" pane, click "Create Basic Task...".

3.  **Task Name and Description:**
    *   **Name:** `Supabase Daily MCP Entry` (or any descriptive name).
    *   **Description:** `Inserts a daily entry into the Supabase 'entry_by_agent' table for MCP server monitoring.`
    *   Click "Next".

4.  **Trigger:**
    *   Select "Daily".
    *   Click "Next".

5.  **Daily Trigger Details:**
    *   **Start:** Set the date and time you want the task to start running (e.g., tomorrow's date, 01:00:00 AM).
    *   **Recur every:** `1` day.
    *   Click "Next".

6.  **Action:**
    *   Select "Start a program".
    *   Click "Next".

7.  **Start a Program Details:**
    *   **Program/script:** Provide the full path to your Python executable. This is typically found within your virtual environment's `Scripts` folder. For example:
        `E:\_Gemini_CLI_TRY\venv\Scripts\python.exe`
        *(Adjust this path based on your actual Python installation and virtual environment location.)*
    *   **Add arguments (optional):** Provide the full path to your Python script:
        `E:\_Gemini_CLI_TRY\MySupabase\supabase_entry.py`
    *   **Start in (optional):** Provide the directory where your script is located and where the `.env` file is accessible:
        `E:\_Gemini_CLI_TRY\MySupabase`
    *   Click "Next".

8.  **Finish:**
    *   Review the summary.
    *   It's recommended to check the box "Open the Properties dialog for this task when I click Finish" to configure advanced settings.
    *   Click "Finish".

9.  **Optional: Advanced Settings (in Properties dialog - if you checked the box in step 8):**
    *   **General Tab:**
        *   For unattended execution (e.g., if you log off), select "Run whether user is logged on or not". You will be prompted for your user password.
    *   **Settings Tab:**
        *   Configure options like what happens if the task fails or misses scheduled runs.

Once configured, the `supabase_entry.py` script will execute daily at your specified time, inserting a new row into your Supabase table.
