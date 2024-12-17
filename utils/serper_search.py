
import http.client
import json
from typing import List, Union

class SerperSearch:
    def __init__(self):
        self.load_keys()
        self.current_key_index = 0
        self.headers = self.update_headers()
        
    def load_keys(self):   
        """Load and sort the keys based on remaining credits"""
        with open('secrets/serper_keys.json', 'r') as f:
            self.key_data = json.load(f)
        self.key_info = self.key_data['key_info']
        self.key_info.sort(key=lambda x: x['count'], reverse=True)
        
    def update_headers(self):
        """Update the headers with the current key"""
        return {
            'X-API-KEY': self.key_info[self.current_key_index]['key'],
            'Content-Type': 'application/json'
        }
        
        
        
    def save_key_count(self, batch_size: int):
        """Update the key count based on the batch size"""
        self.key_info[self.current_key_index]['count'] -= batch_size
        with open('secrets/serper_keys.json', 'w') as f:
            json.dump(self.key_data, f, indent=4)
        
        
        
    def rotate_key_if_needed(self, batch_size: int):
        current_key_credits = self.key_info[self.current_key_index]['count']
        if current_key_credits < batch_size:
            while self.current_key_index < len(self.key_info) - 1:
                self.current_key_index +=1
                if self.key_info[self.current_key_index]['count'] >= batch_size:
                    self.headers = self.update_headers()
                    return True ### Rotated to a key with enough credits
            return False #### No more keys available with enough credits
        return True ### Current key has enough credits
        
    def serper_search(self, query: Union[str, List[str]]) -> Union[dict, List[dict]]:
        conn = http.client.HTTPSConnection("google.serper.dev")
        if isinstance(query, str):
            batch_size = 1
            payload = json.dumps({"q": query})
        else:
            # Handle batch queries
            batch_size = len(query)
            payload = json.dumps(query)
            if batch_size > 100:
                raise ValueError("Batch size cannot be greater than 100")
            
        if not self.rotate_key_if_needed(batch_size):
            raise ValueError("No more keys available with enough credits")
        
        conn.request("POST", "/search", payload, self.headers)
        res = conn.getresponse()
        data = res.read()
        self.save_key_count(batch_size)
        return json.loads(data.decode("utf-8"))
       
    
    
    
# if __name__ == "__main__":
#     api_key = "6ead5f116ca2069f16d03409564d66258f06ff26"
#     batch_queries = [
#         "What is the capital of France?",
#         "What is the capital of Spain?",]
#     query = "What is the capital of France?"
#     result = serper_search(batch_queries, api_key)
#     print(result)


# serper = SerperSearch()
# results = serper.serper_search([{"q": "What is the capital of France?"}, {"q": "What is the capital of Pakistan?"}])
# print(results)