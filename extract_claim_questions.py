from typing import List
import json
from google.generativeai import GenerationConfig
from multiprocessing import Process, Queue, current_process, Manager,Value, Lock
import pandas as pd
from google.generativeai.types import HarmCategory, HarmBlockThreshold
import google.generativeai as genai
import os
from tqdm import tqdm
import threading

import time


# Constants for rate limiting
REQUESTS_PER_MINUTE = 15
REQUESTS_PER_DAY = 1500
SECONDS_PER_REQUEST = 6  # 4 seconds

def load_api_keys(secrets_file: str) -> List[str]:
    with open(secrets_file, "r") as f:
        data = json.load(f)
    return data["keys"]



def save_batch_to_csv(batch: List[dict], output_file: str):
    """Save a batch of records to the CSV file."""
    df_batch = pd.DataFrame(batch)
    try:
        if os.path.exists(output_file):
            df_batch.to_csv(output_file, mode='a', header=False, index=False)
        else:
            df_batch.to_csv(output_file, mode='w', header=True, index=False)
    except Exception as e:
        print(f"Error saving batch to CSV: {e}")
    
        
    

def saver_process(output_queue: Queue, output_file: str, processed_count, lock):
    batch = []
    while True:
        record = output_queue.get()
        if record == "DONE": 
            break
        batch.append(record)
        if len(batch) >= 50:
            save_batch_to_csv(batch, output_file)
            with lock:
                processed_count.value += len(batch)
            batch = []
    if batch:
        save_batch_to_csv(batch, output_file)
        with lock:
            processed_count.value += len(batch)
            


def worker_process(task_queue: Queue, output_queue: Queue, api_key: str, prompt_template: str):
    """Worker process to handle a subset of records using a specific API key."""
    # Configure the API with the assigned key
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(
        "gemini-1.5-flash",
        safety_settings={
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        }
    )
    
    while True: 
        record = task_queue.get()
        if record == "DONE":
            break
        
        record['column'] = None
        
        
        try:
        # Prepare the prompt by replacing placeholders
            claim = record["claim"]
            claim_author = record["claim_author"] if isinstance(record["claim_author"], str) else "UNKNOWN"
            claim_date = record["claim_date"] if isinstance(record["claim_date"], str) else "UNKNOWN"
            claim_type = record["issue"]
            prompt = prompt_template.replace("[Insert the claim here]", claim)
            prompt = prompt.replace("[Insert the claim speaker here]", claim_author)
            prompt = prompt.replace("[Insert the claim date here]", claim_date)
            prompt = prompt.replace("[Insert the claim type here]", claim_type)

            response = model.generate_content(
                prompt,
                generation_config=GenerationConfig(
                temperature=0.3,
                top_p=0.9,
                top_k=40,
                response_mime_type="application/json",
                response_schema={
                "type": "array",
                "items": {
                    "type": "string",
                },
            }
            )
                
            )
            try:
                questions_returned = response.text
            except Exception:
                questions_returned = "[]"
            
            record['questions'] = questions_returned
            output_queue.put(record)
            
            time.sleep(SECONDS_PER_REQUEST)
        except Exception as e:
            print(f"{current_process().name}: Error processing claim at {record["claim"]}: {record['claim']}... Error: {e}")
            output_queue.put(record)


def monitor_progress(processed_count, total, lock):
    """Function to monitor and update the progress bar."""
    with tqdm(total=total, desc="Processed records") as pbar:
        last_count = 0
        while last_count < total:
            with lock:
                current = processed_count.value
            pbar.update(current - last_count)
            last_count = current
            if current >= total:
                break
            time.sleep(1)


def main():
    secrets_file = "secrets/gemini_keys.json"
    api_keys = load_api_keys(secrets_file)
    
    num_processes = min(4, len(api_keys))
    print(f"Starting {num_processes} worker processes")
    
    task_queue = Queue()
    output_queue = Queue()
    
    output_file = "snopes_results_final.csv"
    processed_count = Value('i', 0)
    lock = Lock()
    saver = Process(target=saver_process, args=(output_queue, output_file, processed_count, lock), name="Saver")
    saver.start()
    df = pd.read_csv("snopes_results_with_justification.csv")
    records = df.to_dict('records')

#start worker processes
    processes = []
    with open("prompts/prompt_2Q.txt", "r") as f:
        prompt_template = f.read()
        
    for i in range(num_processes):
        p = Process(
            target=worker_process, 
            args=(task_queue, output_queue, api_keys[i], prompt_template),
            name=f"Worker-{i+1}",

        )
        p.start()
        processes.append(p)
    total_claims = len(df)
        # Start the progress monitoring thread
    progress_thread = threading.Thread(target=monitor_progress, args=(processed_count, total_claims, lock), daemon=True)
    progress_thread.start()
  

    #enqueue all the records 
    for record in tqdm(records, desc="Enqueuing records"):
        task_queue.put(record)


    for _ in range(num_processes):
        task_queue.put("DONE")
        
    for p in processes:
        p.join()
    
    output_queue.put("DONE")
    saver.join()
    print("Processing completed.")

if __name__ == "__main__":
    main()