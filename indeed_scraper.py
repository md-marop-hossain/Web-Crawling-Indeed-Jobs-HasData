import os
import sys
import json
import time
import requests
from datetime import datetime
from urllib.parse import quote, urlparse, parse_qs

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def get_api_key():
    api_key = os.getenv('HASDATA_API_KEY')
    if not api_key:
        print("Error: HASDATA_API_KEY environment variable not set.")
        print("Set HASDATA_API_KEY in your environment or .env file.")
        sys.exit(1)
    return api_key

def get_user_input(job_title, location, limit=None):
    job_title = (job_title or "").strip()
    location = (location or "").strip()
    if not job_title:
        raise ValueError("job_title is required and cannot be empty.")
    if not location:
        raise ValueError("location is required and cannot be empty.")

    if limit is not None:
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            raise ValueError("limit must be an integer (or None).")
        if limit < 1:
            raise ValueError("limit must be >= 1 when provided.")
    return job_title, location, limit

def normalize_job_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        jk = (qs.get("jk") or [None])[0] or (qs.get("vjk") or [None])[0]
        if jk:
            return f"https://www.indeed.com/viewjob?jk={jk}"
        if "/m/viewjob" in url:
            return url.replace("/m/viewjob", "/viewjob", 1)
    except Exception:
        pass
    return url

def fetch_job_listings(api_key, job_title, location, limit=None, domain="www.indeed.com"):
    headers = {'Content-Type': 'application/json', 'x-api-key': api_key}

    collected = []
    seen = set()
    start = 0
    page_idx = 1
    while True:
        url = (
            "https://api.hasdata.com/scrape/indeed/listing"
            f"?keyword={quote(job_title)}&location={quote(location)}&domain={domain}&start={start}"
        )
        print(f"Fetching page {page_idx} of listings (start={start})...")
        try:
            resp = requests.get(url, headers=headers, timeout=30)
        except requests.exceptions.Timeout:
            print("Listing request timed out; stopping.")
            break
        except Exception as e:
            print(f"Listing request error: {e}; stopping.")
            break

        if resp.status_code != 200:
            print(f"Listing API error: {resp.status_code} - {resp.text}; stopping.")
            break

        data = resp.json() or {}
        jobs = data.get("jobs", []) or []
        if not jobs:
            print("No more jobs returned; pagination complete.")
            break

        new_urls = []
        for j in jobs:
            u = j.get("url")
            if not u:
                continue
            if u not in seen:
                seen.add(u)
                new_urls.append(u)

        collected.extend(new_urls)
        print(f"  Received {len(jobs)} items, {len(new_urls)} new; total unique so far: {len(collected)}")

        if limit is not None and len(collected) >= limit:
            collected = collected[:limit]
            print(f"Reached limit={limit}; stopping pagination.")
            break

        start += len(jobs)
        page_idx += 1

        time.sleep(0.6)

    print(f"Found {len(collected)} job URLs (unique).")
    return collected

def fetch_job_details(api_key, job_url):
    headers = {'Content-Type': 'application/json', 'x-api-key': api_key}

    norm = normalize_job_url(job_url)
    url = f"https://api.hasdata.com/scrape/indeed/job?url={quote(norm)}"
    try:
        resp = requests.get(url, headers=headers, timeout=30)
    except Exception as e:
        print(f"Job details request error: {e}")
        return None

    if resp.status_code != 200:
        print(f"Job details API error ({resp.status_code}) for {norm}")
        return None

    data = resp.json() or {}

    def pick(d, *keys, default=""):
        for k in keys:
            if k in d and d[k]:
                return d[k]
        return default

    candidates = [data, data.get("job", {})]

    title = pick(candidates[0], "title") or pick(candidates[1], "title", "jobTitle")
    company = pick(candidates[0], "company") or pick(candidates[1], "company", "companyName", "company_name")
    location = pick(candidates[0], "location") or pick(candidates[1], "location", "jobLocation")
    description = (
        pick(candidates[0], "description", "jobDescription", "full_description")
        or pick(candidates[1], "description", "jobDescription", "full_description")
    )

    return {
        "title": title or "",
        "company": company or "",
        "location": location or "",
        "description": description or "",
        "url": norm,
        "source_url": job_url
    }

def save_jobs_to_json(jobs, job_title, location):
    os.makedirs("data", exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_title_clean = job_title.replace(" ", "_").replace("/", "_")
    location_clean = location.replace(" ", "_").replace(",", "").replace("/", "_")
    filename = f"data/indeed_jobs_{job_title_clean}_{location_clean}_{timestamp}.json"

    result = {
        "metadata": {
            "search_job_title": f"{job_title}_detailed",
            "search_location": location,
            "timestamp": datetime.now().isoformat(),
            "total_jobs": len(jobs)
        },
        "data": {
            "job": jobs
        }
    }

    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"Data successfully saved to: {filename}")
        return filename
    except Exception as e:
        print(f"Error saving file: {str(e)}")
        return None

def scrape_indeed_jobs(job_title, location, job_limit=None):
    print("\n" + "=" * 60)
    print("Starting Job Scraping Process")
    print("=" * 60)
    print(f"Job Title: {job_title}")
    print(f"Location:  {location}")
    print(f"Limit:     {job_limit if job_limit is not None else 'ALL (paginate)'}")
    print("-" * 60)

    api_key = get_api_key()

    job_urls = fetch_job_listings(api_key, job_title, location, limit=job_limit)

    if not job_urls:
        print("No job URLs found. Try different search terms.")
        return

    print(f"\nFetching detailed information for {len(job_urls)} jobs...")
    print("-" * 60)
    detailed = []
    for idx, job_url in enumerate(job_urls, 1):
        print(f"Processing job {idx}/{len(job_urls)}...")
        details = fetch_job_details(api_key, job_url)
        if details:
            detailed.append(details)
            print(f"✓ {details.get('title') or '(no title)'}")
        else:
            print("✗ Failed to fetch job details")
        if idx < len(job_urls):
            time.sleep(0.8)

    if detailed:
        print("\n" + "=" * 60)
        print(f"Saving {len(detailed)} jobs to JSON file...")
        saved_file = save_jobs_to_json(detailed, job_title, location)
        if saved_file:
            print(f"✓ Successfully saved {len(detailed)} jobs")
            print("=" * 60)
            print("Job Scraping Completed Successfully!")
            print("=" * 60)
        else:
            print("✗ Failed to save jobs to file")
    else:
        print("No job details were successfully fetched.")

def main():
    try:
        job_title, location, limit = get_user_input('Machine Learning Engineer', 'Dhaka, Bangladesh', 5)
        scrape_indeed_jobs(job_title, location, limit)

    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
