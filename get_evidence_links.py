import pandas as pd
from tqdm import tqdm
import json
import ast
from utils.serper_search import SerperSearch

class SearchQuery:
    def __init__(self, csv_path: str):
        self.df = pd.read_csv(csv_path)
        self.question_to_claim_id = {}
        self.final_results = {}
        
        
    def prepare_question_mapping(self): 
        """Creating a mapping of question -> claim_id, so "Q1" -> "1" """
        print("Preparing question to claim mapping...")        
        
        for _, row in tqdm(self.df.iterrows(), total=len(self.df)):
            claim_id = row['claim_id']
            claim_text = row['claim']  # Assuming the claim text is in a column named 'claim'
            

            questions = self.parse_questions(row['questions'], claim_id)
            for question in questions:
                question = question.strip()
                self.question_to_claim_id[question] = claim_id
            self.question_to_claim_id[claim_text] = claim_id
                
        print(f"Mapped {len(self.question_to_claim_id)} questions to their claims")
        with open('output/question_to_claim.json', 'w') as f:
            json.dump(self.question_to_claim_id, f, ensure_ascii=False, indent=4)
        return self.question_to_claim_id


    def create_batches(self, batch_size=100):
        """Creating batches of questions to search for"""
        batches = []
        current_batch = []
        for question in self.question_to_claim_id.keys():
            current_batch.append({
                "q": question, 
                "page": 1
            })
            if len(current_batch) == batch_size:
                batches.append(current_batch)
                current_batch = []
        if current_batch:
            batches.append(current_batch)
        # Save batches to JSON file
        with open('output/search_batches.json', 'w') as f:
            json.dump(batches, f, indent=4)
        print(f"Created {len(batches)} batches of size {batch_size}")
        return batches

       
       
       
       
    def parse_questions(self, questions_str, claim_id):
        """Parsing the questions column into a list of strings"""
        if isinstance(questions_str, list):
            return questions_str
            
        try:
            return ast.literal_eval(questions_str)
        except:
            try:
                json_str = questions_str.replace("'", '"')
                return json.loads(json_str)
            except:
                try:
                    return [q.strip(' []"\'') for q in questions_str.split(',')]
                except Exception as e:
                    print(f"Failed to parse questions: {questions_str}, claim_id: {claim_id}")
                    print(f"Error: {e}")
                    return []
                

                
    def process_results(self, results):
        """Processing the results of the search"""
        for result in results:
            question = result["searchParameters"]["q"]
            evidence_links = result["organic"]
            claim_id = self.question_to_claim_id[question]
            
            if claim_id not in self.final_results:
                self.final_results[claim_id] = {}
            self.final_results[claim_id][question] = evidence_links
        
        
    def save_results_to_json(self, file_path):
        """Saving the results to a JSON file"""
        with open(file_path, 'w') as f:
            json.dump(self.final_results, f, ensure_ascii=False,indent=4)
        print(f"Saved {len(self.final_results)} results to {file_path}")
        
                
                
    
        
                
if __name__ == "__main__":
    search_query_manager = SearchQuery("output/snopes_results_latest.csv")
    serper = SerperSearch()
    
    search_query_manager.prepare_question_mapping()
    batches = search_query_manager.create_batches()
    
    for batch_num, batch in enumerate(tqdm(batches, desc="Processing batches")):
        results = serper.serper_search(batch)
        search_query_manager.process_results(results)
    search_query_manager.save_results_to_json('output/search_results.json')