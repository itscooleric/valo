import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import subprocess
import os
import glob


# Scrape matches
num_pages = 5
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
user_name = config.get('user', 'name')
user_email = config.get('user', 'email')

# File paths
valo = {
    "path": {
        "schedule": "./data/schedule",
        "vods": "./data/vods",
    }
}

class VLRScraper:
    def __init__(self):
        self.last_html = None
        self.records_pulled = 0
        self.cache = {}

    def request(self, url):
        if url in self.cache:
            print(f"Using cached content for {url}")
            return self.cache[url]
        else:
            print(f"Fetching content for {url}")
            response = requests.get(url)
            content = response.content
            self.cache[url] = content
            return content
    

    def update_records_pulled(self, count):
        self.records_pulled += count

    def get_last_html(self):
        return self.last_html

    def set_last_html(self, html):
        self.last_html = html
    
vlr = VLRScraper()
def scrape_vlr_matches(num_pages=1):
    """
    Scrape match data from vlr.gg for the specified number of pages.

    Args:
        num_pages (int, optional): The number of pages to scrape. Defaults to 1.

    Returns:
        pd.DataFrame: A DataFrame containing the match data.
    """
    columns = [
        'date', 'match_time', 'team_1', 'team_1_score', 'team_2', 'team_2_score',
        'live_status', 'event_name', 'event_series'
    ]
    df = pd.DataFrame(columns=columns)

    for page in range(1, num_pages + 1):
        url = f"https://www.vlr.gg/matches/?page={page}"
        content = vlr.request(url)
        vlr.set_last_html(content)
        soup = BeautifulSoup(content, "html.parser")

        # Find the wf-card divs
        wf_cards = soup.find_all("div", class_="wf-card", style="margin-bottom: 30px;")

        for wf_card in wf_cards:
            # Extract the date from the date container within the wf-card
            date_container = wf_card.find_previous("div", class_="wf-label mod-large")
            current_date = date_container.get_text(strip=True).split(' ')[-4:]
            current_date = ' '.join(current_date).replace("Today", "").strip()
            current_date = datetime.strptime(current_date, "%a, %B %d, %Y").strftime("%m/%d/%Y")
            # Find the match container elements within the wf-card
            match_containers = wf_card.find_all("a", class_="wf-module-item")

            for match in match_containers:
                # Extract match time
                match_time = match.find("div", class_="match-item-time").get_text(strip=True)
                if match_time.strip() == 'TBD':
                    continue
                match_time = datetime.strptime(match_time, "%I:%M %p").strftime("%H:%M")

                # Extract team names and scores
                teams = match.find_all("div", class_="match-item-vs-team")
                team1_name = teams[0].find("div", class_="text-of").get_text(strip=True)
                team1_score = teams[0].find("div", class_="match-item-vs-team-score").get_text(strip=True)
                team2_name = teams[1].find("div", class_="text-of").get_text(strip=True)
                team2_score = teams[1].find("div", class_="match-item-vs-team-score").get_text(strip=True)

                # Extract live status
                live_status = match.find("div", class_="ml-status")
                live_status = live_status.get_text(strip=True) if live_status else "Not live"

                # Extract event information
                event_series = match.find("div", class_="match-item-event-series").get_text(strip=True)
                event_name = match.find("div", class_="match-item-event").get_text(strip=True).replace(event_series, "").strip()

                # Add extracted information to the DataFrame
                new_row = {
                    'date': current_date,
                    'match_time': match_time,
                    'team_1': team1_name,
                    'team_1_score': team1_score,
                    'team_2': team2_name,
                    'team_2_score': team2_score,
                    'live_status': live_status,
                    'event_name': event_name,
                    'event_series': event_series
                }
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

            # Update records_pulled
            vlr.update_records_pulled(len(match_containers))
        # Log the summary for the current page
        print(f"Page {page} summary:")
        print(f"  - URL: {url}")
        print(f"  - Matches scraped: {len(wf_cards)}")
    # Log the overall summary at the end
    print("\nOverall summary:")
    print(f"  - Total pages scraped: {num_pages}")
    print(f"  - Total matches scraped: {vlr.records_pulled}")
    return df

def get_latest_session_timestamp(archive_folder):
    # List all files in the archive folder with the specific pattern
    file_pattern = os.path.join(archive_folder, "vlr_matches_*.csv")
    files = glob.glob(file_pattern)

    # Check if there are any files in the archive folder
    if not files:
        return None
    
    # Sort files by modification time (newest first)
    files.sort(key=os.path.getmtime, reverse=True)

    # Extract the session timestamp from the latest file's name
    latest_file = files[0]
    session_timestamp = latest_file.split("_")[-1].replace(".csv", "")
    return session_timestamp
    
def update_github_repo(user_name, user_email):
    data_folder = "./schedule"
    archive_folder = os.path.join(data_folder, "archive")

    # Create data and archive folders if they don't exist
    os.makedirs(data_folder, exist_ok=True)
    os.makedirs(archive_folder, exist_ok=True)

    # Get the session timestamp from the latest file in the archive
    session_timestamp = get_latest_session_timestamp(archive_folder)
    if session_timestamp is None:
        print("No archived files found.")
        return

    # Set Git user name and email
    subprocess.run(['git', 'config', '--global', 'user.name', user_name])
    subprocess.run(['git', 'config', '--global', 'user.email', user_email])

    # Add changes in the data folder
    subprocess.run(['git', 'add', f'{data_folder}/*'])

    # Commit changes
    commit_message = f"Data update for session {session_timestamp}"
    subprocess.run(['git', 'commit', '-m', commit_message])

    # Push changes
    subprocess.run(['git', 'push'])

# Scrape match data and add VOD URLs
df = scrape_vlr_matches(num_pages)

# Save the DataFrame to a CSV file
filename = "./data/vlr_matches_with_vods.csv"
df.to_csv(filename, index=False)


# Save the DataFrame to a CSV file
filename = "./data/vlr_matches.csv"
df.to_csv(filename, index=False)

# Push the CSV file to the GitHub repository
update_github_repo(user_name, user_email)
# Save the DataFrame to a csv file with date timestamp as yyyymmdd_hhmmss
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"./data/archive/vlr_matches_{timestamp}.csv"
df.to_csv(filename, index=False)
