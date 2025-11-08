#!/usr/bin/env python3
import json
import pickle
import argparse
import os
from collections import deque

STATE_PICKLE_FILE = "crawl_state.pkl"
STATE_JSON_FILE = "crawl_state.json"

def to_json(output_folder):
    """Converts the pickle state file to a JSON file."""
    pickle_path = os.path.join(output_folder, STATE_PICKLE_FILE)
    json_path = os.path.join(output_folder, STATE_JSON_FILE)

    if not os.path.exists(pickle_path):
        print(f"Error: Pickle file not found at {pickle_path}")
        return

    try:
        with open(pickle_path, 'rb') as f:
            state = pickle.load(f)

        # Convert sets to lists for JSON serialization
        if 'visited_urls_set' in state:
            state['visited_urls_set'] = list(state['visited_urls_set'])
        if 'downloaded_image_urls_set' in state:
            state['downloaded_image_urls_set'] = list(state['downloaded_image_urls_set'])
        if 'pages_to_crawl_queue' in state and isinstance(state['pages_to_crawl_queue'], deque):
            state['pages_to_crawl_queue'] = list(state['pages_to_crawl_queue'])

        with open(json_path, 'w') as f:
            json.dump(state, f, indent=4)

        print(f"Successfully converted {pickle_path} to {json_path}")

    except Exception as e:
        print(f"An error occurred during JSON conversion: {e}")

def to_pickle(output_folder):
    """Converts the JSON state file back to a pickle file."""
    pickle_path = os.path.join(output_folder, STATE_PICKLE_FILE)
    json_path = os.path.join(output_folder, STATE_JSON_FILE)

    if not os.path.exists(json_path):
        print(f"Error: JSON file not found at {json_path}")
        return

    try:
        with open(json_path, 'r') as f:
            state = json.load(f)

        # Convert lists back to sets
        if 'visited_urls_set' in state:
            state['visited_urls_set'] = set(state['visited_urls_set'])
        if 'downloaded_image_urls_set' in state:
            state['downloaded_image_urls_set'] = set(state['downloaded_image_urls_set'])
        if 'pages_to_crawl_queue' in state:
            state['pages_to_crawl_queue'] = deque(state['pages_to_crawl_queue'])

        with open(pickle_path, 'wb') as f:
            pickle.dump(state, f)

        print(f"Successfully converted {json_path} to {pickle_path}")

    except Exception as e:
        print(f"An error occurred during pickle conversion: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert crawl state between pickle and JSON formats.")
    parser.add_argument("mode", choices=["to-json", "to-pickle"], help="The conversion mode.")
    parser.add_argument("output_folder", help="The directory containing the state file.")

    args = parser.parse_args()

    if not os.path.isdir(args.output_folder):
        print(f"Error: The specified output folder does not exist: {args.output_folder}")
    else:
        if args.mode == "to-json":
            to_json(args.output_folder)
        elif args.mode == "to-pickle":
            to_pickle(args.output_folder)
