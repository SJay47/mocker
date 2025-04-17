from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from faker import Faker

fake = Faker()

# API Configuration
API_CONFIG = {
    "API_BASE_URL": "http://localhost",
    "KEYCLOAK_TOKEN_URL": "http://localhost:18080/realms/primecare/protocol/openid-connect/token",
    "KEYCLOAK_CLIENT_ID": "swagger-client-primecare",
    "ADMIN_USERNAME": "alice@demo.com",
    "ADMIN_PASSWORD": "q",
    "ORGANIZATION_ID": "fbb95a46-0186-4087-88a2-f602bbaafb28",
    "DATASET_ID": "117e5209-ce37-46cc-9177-5092c9fbbe24",
}


def fake_recordset_fields(data):
    """
    Recursively replace every string value in a recordSet with a fake value.
    """
    if isinstance(data, dict):
        new_dict = {}
        for k, v in data.items():
            if isinstance(v, str):
                new_dict[k] = fake.bs()
            elif isinstance(v, dict | list):
                new_dict[k] = fake_recordset_fields(v)
            else:
                new_dict[k] = v
        return new_dict
    if isinstance(data, list):
        return [
            fake_recordset_fields(item)
            if isinstance(item, dict | list)
            else (fake.bs() if isinstance(item, str) else item)
            for item in data
        ]
    return fake.bs() if isinstance(data, str) else data


def custom_generate(data):
    """
    Process the JSON data to mock ONLY:
    - statistics and percentiles fields with random integer values (min and max values range from 0-100)

    All other fields are left unchanged, including:
    - fingerprint (name, description)
    - recordSet and its nested fields
    - fields and their properties

    Preserves the original value of datasetId at any level.
    """
    if isinstance(data, dict):
        new_data = {}
        for key, value in data.items():
            # Handle statistics fields
            if key.lower() == "statistics":
                if isinstance(value, dict):
                    # Create new statistics with min and max values between 0-100
                    min_val = fake.random_int(min=0, max=40)
                    max_val = fake.random_int(min=60, max=100)

                    # Generate random statistics values based on min and max
                    mean_val = fake.random_int(min=min_val + 10, max=max_val - 10)
                    median_val = fake.random_int(min=min_val + 5, max=max_val - 5)
                    std_dev = fake.random_int(min=5, max=20)
                    unique_count = fake.random_int(min=50, max=95)
                    null_count = fake.random_int(min=0, max=20)

                    new_stats = {}
                    for stat_key, stat_value in value.items():
                        if stat_key.lower() == "min":
                            new_stats[stat_key] = min_val
                        elif stat_key.lower() == "max":
                            new_stats[stat_key] = max_val
                        elif stat_key.lower() == "mean":
                            new_stats[stat_key] = mean_val
                        elif stat_key.lower() == "median":
                            new_stats[stat_key] = median_val
                        elif stat_key.lower() == "stddev":
                            new_stats[stat_key] = std_dev
                        elif stat_key.lower() == "uniquecount":
                            new_stats[stat_key] = unique_count
                        elif stat_key.lower() == "nullcount":
                            new_stats[stat_key] = null_count
                        elif stat_key.lower() == "percentiles":
                            # Generate random percentile values that make sense (ascending order)
                            # Ensure we have enough range between values
                            range_size = max_val - min_val

                            # Divide the range into four parts to ensure ascending values
                            p25_max = min_val + (range_size // 3)
                            p25 = fake.random_int(min=min_val, max=p25_max)

                            p50_min = p25 + 1
                            p50_max = min_val + (2 * range_size // 3)
                            p50 = fake.random_int(min=p50_min, max=p50_max)

                            p75_min = p50 + 1
                            p75 = fake.random_int(min=p75_min, max=max_val)

                            if isinstance(stat_value, dict):
                                new_percentiles = {}
                                for perc_key, perc_value in stat_value.items():
                                    if perc_key.lower() == "p25":
                                        new_percentiles[perc_key] = p25
                                    elif perc_key.lower() == "p50":
                                        new_percentiles[perc_key] = p50
                                    elif perc_key.lower() == "p75":
                                        new_percentiles[perc_key] = p75
                                    else:
                                        new_percentiles[perc_key] = perc_value
                                new_stats[stat_key] = new_percentiles
                            else:
                                new_stats[stat_key] = stat_value
                        else:
                            # For any other statistics fields, keep them as is
                            new_stats[stat_key] = stat_value
                    new_data[key] = new_stats
                else:
                    new_data[key] = value
            # Handle percentiles fields
            elif key.lower() == "percentiles":
                if isinstance(value, dict):
                    # Generate random percentile values that make sense (ascending order)
                    p25 = fake.random_int(min=10, max=30)
                    p50 = fake.random_int(min=p25 + 1, max=60)
                    p75 = fake.random_int(min=p50 + 1, max=90)

                    new_percentiles = {}
                    for perc_key, perc_value in value.items():
                        if perc_key.lower() == "p25":
                            new_percentiles[perc_key] = p25
                        elif perc_key.lower() == "p50":
                            new_percentiles[perc_key] = p50
                        elif perc_key.lower() == "p75":
                            new_percentiles[perc_key] = p75
                        else:
                            new_percentiles[perc_key] = perc_value
                    new_data[key] = new_percentiles
                else:
                    new_data[key] = value
            # For all other fields, preserve them but process any nested dictionaries or lists
            else:
                if isinstance(value, dict):
                    new_data[key] = custom_generate(value)
                elif isinstance(value, list):
                    new_data[key] = [
                        custom_generate(item) if isinstance(item, dict | list) else item
                        for item in value
                    ]
                else:
                    # Keep all other values unchanged
                    new_data[key] = value
        return new_data
    if isinstance(data, list):
        return [
            custom_generate(item) if isinstance(item, dict | list) else item
            for item in data
        ]
    return data  # Return primitive values unchanged


def save_mocked_fingerprint(fp: dict, filename: str):
    """
    Save the fingerprint JSON to a file inside the 'mock_fingerprints' folder.
    """
    output_dir = Path("mock_fingerprints")
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / filename
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(fp, f, indent=2)
    return file_path


def read_json_template(file_path: Path) -> dict:
    """
    Read a JSON file and return its contents as a dictionary.
    """
    try:
        with file_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {file_path}: {e}")
        return {}
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return {}


def get_access_token() -> Optional[str]:
    """
    Authenticate with Keycloak and get an access token.
    """
    token_url = API_CONFIG["KEYCLOAK_TOKEN_URL"]
    client_id = API_CONFIG["KEYCLOAK_CLIENT_ID"]
    username = API_CONFIG["ADMIN_USERNAME"]
    password = API_CONFIG["ADMIN_PASSWORD"]

    payload = {
        "client_id": client_id,
        "grant_type": "password",
        "username": username,
        "password": password,
    }

    try:
        response = requests.post(token_url, data=payload)
        response.raise_for_status()
        token_data = response.json()
        return token_data.get("access_token")
    except requests.RequestException as e:
        print(f"Authentication error: {e}")
        return None


def post_fingerprint(fingerprint: Dict[str, Any], access_token: str) -> bool:
    """
    POST the fingerprint to the API.
    """
    organization_id = API_CONFIG["ORGANIZATION_ID"]
    dataset_id = API_CONFIG["DATASET_ID"]
    api_url = f"{API_CONFIG['API_BASE_URL']}/api/organizations/{organization_id}/datasets/{dataset_id}/fingerprints"

    # Ensure the fingerprint data contains the dataset ID
    if "data" in fingerprint and isinstance(fingerprint["data"], dict):
        fingerprint["data"]["datasetId"] = dataset_id

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(api_url, json=fingerprint, headers=headers)
        response.raise_for_status()
        print(f"Successfully posted fingerprint. Response: {response.status_code}")
        return True
    except requests.RequestException as e:
        print(f"Error posting fingerprint: {e}")
        if hasattr(e, "response") and e.response:
            print(f"Response status: {e.response.status_code}")
            print(f"Response body: {e.response.text}")
        return False


def parse_arguments():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(description="Generate and send mock fingerprints.")
    parser.add_argument(
        "--count",
        "-c",
        type=int,
        default=None,
        help="Number of mock fingerprints to generate from each template.",
    )
    parser.add_argument(
        "--send",
        "-s",
        action="store_true",
        help="Send the generated fingerprints to the API.",
    )
    parser.add_argument(
        "--templates-dir",
        "-t",
        type=str,
        default="templates",
        help="Directory containing the template JSON files.",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=str,
        default="mock_fingerprints",
        help="Directory to save the generated mock fingerprints.",
    )

    return parser.parse_args()


def main():
    # Parse command line arguments
    args = parse_arguments()

    # If count is not provided, ask the user
    mock_count = args.count
    if mock_count is None:
        try:
            mock_count = int(
                input(
                    "Enter the number of mock fingerprints to generate from each template: "
                )
            )
            if mock_count <= 0:
                print("Number of mocks must be greater than 0.")
                return
        except ValueError:
            print("Please enter a valid number.")
            return

    # Define the templates directory
    templates_dir = Path(args.templates_dir)
    output_dir = Path(args.output_dir)

    # Check if templates directory exists
    if not templates_dir.exists():
        print(f"Templates directory '{templates_dir}' not found. Creating it...")
        templates_dir.mkdir(parents=True, exist_ok=True)
        print(f"Please add JSON template files to the '{templates_dir}' directory.")
        return

    # Get all JSON files from the templates directory
    json_files = list(templates_dir.glob("*.json"))

    if not json_files:
        print(f"No JSON files found in '{templates_dir}' directory.")
        print("Please add JSON template files to continue.")
        return

    print(f"Found {len(json_files)} JSON template(s) to process.")
    print(f"Will generate {mock_count} mock(s) from each template.")

    # Get access token for API authentication if sending is enabled
    access_token = None
    if args.send:
        access_token = get_access_token()
        if not access_token:
            print("Failed to obtain access token. API integration will be skipped.")
            print("Continuing with local file generation only...")
        else:
            print("Successfully obtained access token.")

    # Get organization and dataset IDs from config
    organization_id = API_CONFIG["ORGANIZATION_ID"]
    dataset_id = API_CONFIG["DATASET_ID"]
    print(f"Using Organization ID: {organization_id}")
    print(f"Using Dataset ID: {dataset_id}")

    # Create a list to store all generated fingerprints
    all_fingerprints = []

    # Process each JSON file
    for json_file in json_files:
        print(f"Processing template: {json_file.name}")

        # Read the template
        template_data = read_json_template(json_file)
        if not template_data:
            print(f"Skipping empty or invalid template: {json_file.name}")
            continue

        # Generate multiple mocks from the template
        for i in range(mock_count):
            print(f"  Generating mock {i + 1}/{mock_count} from {json_file.name}")

            # Ensure the template has the dataset ID
            if "data" in template_data and isinstance(template_data["data"], dict):
                template_data["data"]["datasetId"] = dataset_id

            # Process the template data with custom_generate
            mocked_data = custom_generate(template_data)

            # Save the mocked fingerprint to a file
            output_filename = f"mocked_{json_file.stem}_{i + 1}.json"
            file_path = save_mocked_fingerprint(mocked_data, output_filename)
            print(f"  Mock fingerprint saved to '{output_dir}/{output_filename}'.")

            # Add to the list of all fingerprints
            all_fingerprints.append(mocked_data)

    # Post all fingerprints to the API if we have an access token and sending is enabled
    if access_token and args.send and all_fingerprints:
        print(f"\nSending {len(all_fingerprints)} mock fingerprints to the API...")
        success_count = 0

        for i, fingerprint in enumerate(all_fingerprints):
            print(f"  Posting fingerprint {i + 1}/{len(all_fingerprints)}...")
            success = post_fingerprint(fingerprint, access_token)
            if success:
                success_count += 1

        print(
            f"\nSent {success_count} out of {len(all_fingerprints)} fingerprints successfully."
        )

    print("\nAll templates processed successfully.")


if __name__ == "__main__":
    main()
