import os
from pathlib import Path
from openai import OpenAI
from dotenv import load_dotenv

dotenv_path = Path('./env')
load_dotenv()

client = OpenAI()

def call_chatgpt_api(prompt: str) -> str:
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[

                {"role": "system", "content": "You are an SAP license optimization assistant."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            timeout=600
        )
        if response.choices:
            return response.choices[0].message.content.strip()
        else:
            return "Error: No response from OpenAI API."

    except Exception as e:
        error_message = f"Error calling OpenAI API: {e}"
        print(error_message)
        return error_message

