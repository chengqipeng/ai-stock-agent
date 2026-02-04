import requests
import re

def call_gemini_api(text):
    url = "https://api2.aigcbest.top/v1beta/models/gemini-3-pro-all:generateContent?key=sk-F6CFwjNNJPotsZqZkEVaws1d4VGUTjg7KlZEJe5dbPmFCFOb"
    headers = {"Content-Type": "application/json"}
    data = {"contents": [{"parts": [{"text": text}]}]}
    
    response = requests.post(url, headers=headers, json=data)
    return response.json()

def parse_response(result):
    text = result['candidates'][0]['content']['parts'][0]['text']
    think_match = re.search(r'<think>(.*?)</think>', text, re.DOTALL)
    think = think_match.group(1).strip() if think_match else ""
    content = re.sub(r'<think>.*?</think>\n?', '', text, flags=re.DOTALL).strip()
    return {"think": think, "content": content}

if __name__ == "__main__":
    result = call_gemini_api("销售易创始人")
    parsed = parse_response(result)
    print("Think:", parsed["think"])
    print("\nContent:", parsed["content"])
