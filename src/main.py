"""
AutoScout24.ch API Scraper — Apify Actor
=========================================
Scrapes car listings from AutoScout24.ch via their internal API.
Runs 8 parallel tasks covering pages 0-8000, deduplicated via a shared set.

Output fields per listing:
- listing_id, title, make_name, model_name
- price, previous_price, mileage
- first_registration_date, power_kw, power_ps
- fuel_type, transmission, consumption_combined
- condition_type, version_fullname
- seller_name, seller_city, seller_zipcode
- created_date, last_modified_date
- images_online (count), image_keys (list of image URLs)
- url

Author: AutoDataScrape
"""

import httpx
import asyncio
import json
import random
import time
import traceback
from datetime import datetime
from apify import Actor

# =============================================================================
# CONSTANTS
# =============================================================================

API_URL            = "https://api.autoscout24.ch/v1/listings/search"
BASE_URL           = "https://www.autoscout24.ch"
IMAGE_BASE_URL     = "https://listing-images.autoscout24.ch/"

API_HEADERS = {
    "accept":             "*/*",
    "accept-language":    "de-CH,de;q=0.9,en;q=0.8",
    "content-type":       "application/json",
    "origin":             BASE_URL,
    "priority":           "u=1, i",
    "referer":            BASE_URL + "/",
    "sec-ch-ua":          '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-site",
    "user-agent":         (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
}

# =============================================================================
# DEDUPLICATION — shared across 8 parallel tasks
# =============================================================================

PROCESSED_IDS:      set          = set()
PROCESSED_IDS_LOCK: asyncio.Lock = asyncio.Lock()

# =============================================================================
# DATA EXTRACTION
# =============================================================================

def extract_listing(item: dict) -> dict | None:
    """
    Extracts and normalizes a single listing from the API response.
    Returns None if listing_id or title is missing.
    """
    listing_id = item.get("id")
    if not listing_id:
        return None

    # --- Make ---
    make_data = item.get("make", {})
    make_name = make_data.get("name", "") if isinstance(make_data, dict) else ""
    make_key  = make_data.get("key",  "") if isinstance(make_data, dict) else ""

    # --- Model ---
    model_data = item.get("model", {})
    model_name = model_data.get("name", "") if isinstance(model_data, dict) else ""
    model_key  = model_data.get("key",  "") if isinstance(model_data, dict) else ""

    # --- Title ---
    version_fullname = item.get("versionFullName", "")
    title = item.get("teaser") or f"{make_name} {model_name} {version_fullname}".strip()
    if not title:
        return None

    # --- Fuel type normalization ---
    fuel_raw = item.get("fuelType", "")
    fuel_map = {
        "benzin":  "Petrol",
        "diesel":  "Diesel",
        "elektro": "Electric",
        "hybrid":  "Hybrid",
    }
    fuel_type = fuel_map.get(fuel_raw.lower() if fuel_raw else "", fuel_raw)

    # --- Transmission normalization ---
    trans_raw = item.get("transmissionType", "")
    trans_map = {
        "automatikgetriebe": "Automatic",
        "automatik":         "Automatic",
        "automatic":         "Automatic",
        "schaltgetriebe":    "Manual",
        "manuell":           "Manual",
        "manual":            "Manual",
        "semi-automatic":    "Automatic",
    }
    transmission = trans_map.get(trans_raw.lower() if trans_raw else "", trans_raw)

    # --- Consumption ---
    consumption_data     = item.get("consumption", {})
    consumption_combined = (
        consumption_data.get("combined")
        if isinstance(consumption_data, dict)
        else None
    )

    # --- Seller ---
    seller_data  = item.get("seller", {})
    seller_name  = seller_data.get("name")    if isinstance(seller_data, dict) else None
    seller_city  = seller_data.get("city")    if isinstance(seller_data, dict) else None
    seller_zip   = seller_data.get("zipCode") if isinstance(seller_data, dict) else None

    # --- Images ---
    images_raw  = item.get("images", [])
    image_keys  = [
        IMAGE_BASE_URL + img["key"]
        for img in images_raw
        if isinstance(img, dict) and img.get("key")
    ]

    # --- URL ---
    if make_key and model_key:
        url = f"{BASE_URL}/de/auto/{make_key.lower()}/{model_key.lower()}?vehid={listing_id}"
    else:
        url = f"{BASE_URL}/de/auto?vehid={listing_id}"

    return {
        "listing_id":           listing_id,
        "title":                title,
        "make_name":            make_name or None,
        "model_name":           model_name or None,
        "version_fullname":     version_fullname or None,
        "condition_type":       item.get("conditionType"),
        "price":                item.get("price"),
        "previous_price":       item.get("previousPrice"),
        "mileage":              item.get("mileage"),
        "first_registration_date": item.get("firstRegistrationDate"),
        "power_kw":             item.get("kiloWatts"),
        "power_ps":             item.get("horsePower"),
        "fuel_type":            fuel_type or None,
        "transmission":         transmission or None,
        "consumption_combined": consumption_combined,
        "seller_name":          seller_name,
        "seller_city":          seller_city,
        "seller_zipcode":       seller_zip,
        "created_date":         item.get("createdDate"),
        "last_modified_date":   item.get("lastModifiedDate"),
        "images_online":        len(image_keys),
        "image_keys":           image_keys,
        "url":                  url,
    }


# =============================================================================
# TASK — one of 8 parallel workers
# =============================================================================

async def run_task(
    client:         httpx.AsyncClient,
    task_id:        str,
    start_page:     int,
    max_pages:      int,
    page_size:      int,
    min_delay:      float,
    max_delay:      float,
    write_interval: int,
) -> int:
    """
    Scrapes one segment of pages (e.g. pages 0-1000).
    Returns total number of unique listings found.
    """
    data_chunk:        list = []
    pages_since_write: int  = 0
    total_unique:      int  = 0

    base_payload = {
        "query": {"vehicleCategories": ["car"]},
        "sort":  [{"order": "ASC", "type": "PRICE"}],
    }

    for i in range(max_pages):
        page_num = start_page + i + 1  # API is 1-indexed

        payload = {
            **base_payload,
            "pagination": {"size": page_size, "page": page_num},
        }

        # --- HTTP request with 3 retries ---
        response = None
        for attempt in range(3):
            try:
                await asyncio.sleep(random.uniform(min_delay, max_delay))
                response = await client.post(
                    API_URL,
                    headers=API_HEADERS,
                    json=payload,
                    timeout=30.0,
                )
                response.raise_for_status()
                break
            except (httpx.ConnectError, httpx.ReadError,
                    httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                Actor.log.warning(f"[{task_id}] Page {page_num} attempt {attempt + 1}: {e}")
                if attempt < 2:
                    await asyncio.sleep(5 * (attempt + 1))
                else:
                    response = None
            except Exception as e:
                Actor.log.error(f"[{task_id}] Unexpected error on page {page_num}: {e}")
                response = None
                break

        if response is None:
            continue

        # --- Parse response ---
        try:
            data     = response.json()
            listings = data.get("content", [])

            if not listings:
                Actor.log.info(f"[{task_id}] Empty page {page_num} — stopping task")
                break

            for item in listings:
                record = extract_listing(item)
                if not record:
                    continue

                # Deduplicate across all 8 parallel tasks
                async with PROCESSED_IDS_LOCK:
                    if record["listing_id"] in PROCESSED_IDS:
                        continue
                    PROCESSED_IDS.add(record["listing_id"])

                data_chunk.append(record)
                total_unique += 1

            Actor.log.info(
                f"[{task_id}] Page {page_num} — "
                f"{len(listings)} listings | "
                f"task total: {total_unique}"
            )

            pages_since_write += 1

            # Batch push to dataset
            if pages_since_write >= write_interval and data_chunk:
                await Actor.push_data(data_chunk)
                Actor.log.info(f"[{task_id}] Pushed {len(data_chunk)} records to dataset")
                data_chunk        = []
                pages_since_write = 0

            # API signals last page
            if data.get("last", False):
                Actor.log.info(f"[{task_id}] API signaled last page at {page_num}")
                break

        except json.JSONDecodeError:
            Actor.log.warning(f"[{task_id}] Failed to parse JSON on page {page_num}")
            continue
        except Exception as e:
            Actor.log.error(f"[{task_id}] Error processing page {page_num}: {e}")
            traceback.print_exc()
            break

    # Final flush
    if data_chunk:
        await Actor.push_data(data_chunk)
        Actor.log.info(f"[{task_id}] Final flush: {len(data_chunk)} records")

    Actor.log.info(f"[{task_id}] Finished — {total_unique} unique listings")
    return total_unique


# =============================================================================
# MAIN — Actor entry point
# =============================================================================

async def main() -> None:
    async with Actor:

        # --- Read input ---
        inp              = await Actor.get_input() or {}
        max_pages        = inp.get("maxPagesPerTask",  1000)
        page_size        = inp.get("pageSizeApi",        20)
        min_delay        = inp.get("minRequestDelay",   1.0)
        max_delay        = inp.get("maxRequestDelay",   2.5)
        write_interval   = inp.get("writeInterval",     100)

        Actor.log.info(
            f"Starting AutoScout24.ch scraper | "
            f"maxPagesPerTask={max_pages} | "
            f"pageSize={page_size} | "
            f"delay={min_delay}-{max_delay}s | "
            f"writeInterval={write_interval}"
        )

        # --- Reset dedup set ---
        PROCESSED_IDS.clear()

        # --- 8 parallel tasks, each covers 1000 pages ---
        tasks_config = [
            {"id": f"Task{i + 1}", "start_page": i * 1000}
            for i in range(8)
        ]

        start_time = time.time()

        await Actor.set_status_message(
            f"Running {len(tasks_config)} parallel tasks..."
        )

        limits = httpx.Limits(
            max_connections=100,
            max_keepalive_connections=20,
        )

        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=30.0,
            limits=limits,
        ) as client:

            results = await asyncio.gather(
                *[
                    run_task(
                        client         = client,
                        task_id        = t["id"],
                        start_page     = t["start_page"],
                        max_pages      = max_pages,
                        page_size      = page_size,
                        min_delay      = min_delay,
                        max_delay      = max_delay,
                        write_interval = write_interval,
                    )
                    for t in tasks_config
                ],
                return_exceptions=True,
            )

        # --- Final summary ---
        elapsed    = time.time() - start_time
        total_ads  = sum(r for r in results if isinstance(r, int))
        errors     = sum(1 for r in results if isinstance(r, Exception))

        Actor.log.info(
            f"Completed in {elapsed:.0f}s | "
            f"Total unique listings: {total_ads:,} | "
            f"Task errors: {errors}"
        )

        await Actor.set_status_message(
            f"Done — {total_ads:,} listings scraped in {elapsed:.0f}s"
        )