import logging
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO)

def invoke_llm_endpoint(prompts_data: list[dict], endpoint_url: str, api_key: str) -> list[dict]:
    """
    Invokes the LLM serving endpoint in parallel to generate summaries.
    """
    summaries = []
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    def post_request(prompt_data):
        try:
            payload = {"prompt": prompt_data["prompt"]} # Varies based on serving API
            response = requests.post(endpoint_url, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
            return {"product_id": prompt_data["product_id"], "summary": response.json()["summary"]}
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to get summary for product {prompt_data['product_id']}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_prompt = {executor.submit(post_request, p): p for p in prompts_data}
        for future in as_completed(future_to_prompt):
            result = future.result()
            if result:
                summaries.append(result)

    logging.info(f"Successfully generated {len(summaries)} summaries.")
    return summaries