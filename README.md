# DSLD Supplement Data Uploader 💊
## What This Script Does
This Python script automates the process of populating a Supabase database with the latest supplement information from the National Institutes of Health (NIH) Dietary Supplement Label Database (DSLD).

It goes to the official NIH download link, downloads the complete database as a zip file, and extracts it in memory. It then intelligently finds all the necessary product information, combines it, and uploads it directly to your Supabase backend. This creates a comprehensive, searchable database of supplements for use in the Supplement Advisor application.

## Key Features:
### Automated Download ⚙️: 
Fetches the latest version of the DSLD database directly from the source URL.

### In-Memory Processing 🧠: 
Unzips the archive and processes the CSV files without writing temporary files to your disk.

### Smart File Combination 🔋: 
Scans the archive for all files containing "ProductOverview" in their name and combines them into a single, unified dataset.

### Data Cleaning 🧹: 
Prepares the data for upload by handling empty values to prevent database errors.

### Efficient Supabase Upload ⚡: 
Connects to your Supabase project and uploads the data in manageable batches to prevent timeouts and network errors. It uses an upsert operation to avoid creating duplicate entries if the script is run multiple times.

## Setup
Before running the script, you need to set up your environment.

### Install Dependencies:
Make sure you have Python installed, then run the following command in your terminal to install the required libraries:

pip install requests pandas supabase python-dotenv numpy

### Create .env File:
In the same directory as the script, create a file named .env. This file will securely store your Supabase credentials. Add the following lines, replacing the placeholder values with your actual Supabase URL and Service Role Key:

```SUPABASE_URL="YOUR_SUPABASE_URL"```
```SUPABASE_SERVICE_KEY="YOUR_SUPABASE_SERVICE_ROLE_KEY"```

### How to Run
Once the setup is complete, you can run the script from your terminal with a single command:

`python main.py`

The script will print its progress to the console, from downloading the data to uploading the final batches to your database.