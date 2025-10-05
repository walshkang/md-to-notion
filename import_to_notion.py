import os
import requests
import json
import re
import time

# --- CONFIGURATION (Final Version - Fully Configured) ---
NOTION_API_KEY = os.getenv("NOTION_API_KEY")

# -- For your EPICS database --
EPIC_TITLE_PROPERTY = "Project name"
EPIC_STATUS_PROPERTY = "Status"
EPIC_DEFAULT_STATUS = "In Progress"

# -- For your SPRINTS database --
SPRINT_TITLE_PROPERTY = "Sprint name"
SPRINT_EPIC_RELATION_PROPERTY = "Epic"

# -- For your TASKS database --
TASK_TITLE_PROPERTY = "Task name"
TASK_STATUS_PROPERTY = "Status"
TASK_DEFAULT_STATUS = "Not Started"
TASK_SPRINT_RELATION_PROPERTY = "Sprint"
TASK_EPIC_RELATION_PROPERTY = "Epic"
# --------------------------------------------------------------------

# Database IDs
EPICS_DB_ID = "2832612c707781688281e47919d32a83"
SPRINTS_DB_ID = "2832612c70778131b51af33fecd7c00b"
TASKS_DB_ID = "2832612c7077811f808ae1d5a77e74ac"

# API Constants
NOTION_API_BASE_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"
MARKDOWN_FILE = "notion_plan.md"

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": NOTION_VERSION,
}

def query_database(database_id: str, title_property: str) -> dict:
    """Queries a Notion database and returns a dictionary of pages mapped by title."""
    pages = {}
    url = f"{NOTION_API_BASE_URL}/databases/{database_id}/query"
    has_more = True
    next_cursor = None

    while has_more:
        payload = {}
        if next_cursor:
            payload["start_cursor"] = next_cursor
        
        try:
            response = requests.post(url, headers=HEADERS, json=payload)
            response.raise_for_status()
            data = response.json()

            for page in data["results"]:
                # Check if the title property is not empty and has content
                if title_property in page["properties"] and page["properties"][title_property]["title"]:
                    title = page["properties"][title_property]["title"][0]["plain_text"]
                    pages[title] = page["id"]

            has_more = data["has_more"]
            next_cursor = data["next_cursor"]
        except requests.exceptions.RequestException as e:
            print(f"❌ Error querying database {database_id}: {e}")
            return {}
            
    return pages

def create_notion_page(database_id: str, properties: dict) -> dict:
    """Creates a new page in a specified Notion database."""
    payload = {"parent": {"database_id": database_id}, "properties": properties}
    try:
        response = requests.post(f"{NOTION_API_BASE_URL}/pages", headers=HEADERS, data=json.dumps(payload))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ API Request Failed during creation: {e}")
        if e.response: print(f"Notion API Response: {e.response.text}")
        return None

def archive_notion_page(page_id: str):
    """Archives a Notion page."""
    url = f"{NOTION_API_BASE_URL}/pages/{page_id}"
    payload = {"archived": True}
    try:
        response = requests.patch(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        print(f"  🗑️ Archived page: {page_id}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Error archiving page {page_id}: {e}")


def sync_plan_to_notion(file_path: str):
    """Reads a markdown file and syncs its content with Notion databases."""
    if not os.path.exists(file_path):
        print(f"Error: Markdown file not found at '{file_path}'")
        return

    print("Step 1: Fetching existing data from Notion...")
    existing_epics = query_database(EPICS_DB_ID, EPIC_TITLE_PROPERTY)
    existing_sprints = query_database(SPRINTS_DB_ID, SPRINT_TITLE_PROPERTY)
    existing_tasks = query_database(TASKS_DB_ID, TASK_TITLE_PROPERTY)
    print("... Fetch complete.")

    # --- Read Markdown and track items ---
    markdown_epics, markdown_sprints, markdown_tasks = set(), set(), set()
    current_epic_title, current_sprint_title = "", ""
    
    with open(file_path, 'r') as f:
        for line in f:
            if line.strip().startswith("### Epic:"):
                current_epic_title = line.replace("### Epic:", "").strip()
                markdown_epics.add(current_epic_title)
            elif line.strip().startswith("#### Sprint"):
                current_sprint_title = line.replace("####", "").strip()
                markdown_sprints.add(current_sprint_title)
            elif line.strip().startswith("- [ ]"):
                task_title = line.replace("- [ ]", "").strip()
                markdown_tasks.add(task_title)

    print("\nStep 2: Syncing Epics, Sprints, and Tasks...")
    current_epic_id, current_sprint_id = "", ""
    
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or "***" in line or "---" in line: continue

            if line.startswith("### Epic:"):
                title = line.replace("### Epic:", "").strip()
                if title in existing_epics:
                    print(f"  - Found Epic: '{title}'")
                    current_epic_id = existing_epics[title]
                else:
                    properties = {
                        EPIC_TITLE_PROPERTY: {"title": [{"text": {"content": title}}]},
                        EPIC_STATUS_PROPERTY: {"status": {"name": EPIC_DEFAULT_STATUS}},
                    }
                    new_item = create_notion_page(EPICS_DB_ID, properties)
                    if new_item:
                        current_epic_id = new_item['id']
                        print(f"  ✅ Created Epic: '{title}'")
                        time.sleep(0.4)
            
            elif line.startswith("#### Sprint"):
                title = line.replace("####", "").strip()
                if title in existing_sprints:
                    print(f"    - Found Sprint: '{title}'")
                    current_sprint_id = existing_sprints[title]
                else:
                    properties = { SPRINT_TITLE_PROPERTY: {"title": [{"text": {"content": title}}]} }
                    if current_epic_id:
                        properties[SPRINT_EPIC_RELATION_PROPERTY] = {"relation": [{"id": current_epic_id}]}
                    new_item = create_notion_page(SPRINTS_DB_ID, properties)
                    if new_item:
                        current_sprint_id = new_item['id']
                        print(f"    ✅ Created Sprint: '{title}'")
                        time.sleep(0.4)
            
            elif line.startswith("- [ ]"):
                title = line.replace("- [ ]", "").strip()
                if title in existing_tasks:
                    print(f"      - Found Task: '{title}'")
                else:
                    properties = {
                        TASK_TITLE_PROPERTY: {"title": [{"text": {"content": title}}]},
                        TASK_STATUS_PROPERTY: {"status": {"name": TASK_DEFAULT_STATUS}},
                    }
                    if current_sprint_id:
                        properties[TASK_SPRINT_RELATION_PROPERTY] = {"relation": [{"id": current_sprint_id}]}
                    if current_epic_id:
                        properties[TASK_EPIC_RELATION_PROPERTY] = {"relation": [{"id": current_epic_id}]}
                    new_item = create_notion_page(TASKS_DB_ID, properties)
                    if new_item:
                        print(f"      ✅ Created Task: '{title}'")
                        time.sleep(0.4)

    print("\nStep 3: Archiving items removed from the plan...")
    for epic_title, page_id in existing_epics.items():
        if epic_title not in markdown_epics:
            archive_notion_page(page_id)
    
    for sprint_title, page_id in existing_sprints.items():
        if sprint_title not in markdown_sprints:
            archive_notion_page(page_id)

    for task_title, page_id in existing_tasks.items():
        if task_title not in markdown_tasks:
            archive_notion_page(page_id)

    print("\n🎉 Notion sync complete!")

if __name__ == "__main__":
    if not NOTION_API_KEY:
        print("Error: NOTION_API_KEY environment variable not set.")
    else:
        sync_plan_to_notion(MARKDOWN_FILE)