

import os
from pathlib import Path
from openai import OpenAI
from anthropic import Anthropic
from dotenv import load_dotenv

from app.core.logger import logger

dotenv_path = Path('./env')
load_dotenv()

# 1 = ChatGPT, 2 = Claude
AI_PROVIDER = 2

openai_client = OpenAI()
anthropic_client = Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
def call_ai_api(prompt: str) -> str:

    try:
        if AI_PROVIDER == 1:
            model_name = "gpt-4o-mini"
            logger.info(f"Calling AI: ChatGPT model '{model_name}'")
            return call_chatgpt_api(prompt)
        elif AI_PROVIDER == 2:
            model_name = "claude-opus-4-20250514"
            logger.info(f"Calling AI: Claude model '{model_name}'")
            return call_claude_api(prompt)
        else:
            logger.error(f"Error: Invalid AI_PROVIDER value ({AI_PROVIDER}). Use 1 for ChatGPT or 2 for Claude.")
            return "Error: Invalid AI_PROVIDER value. Use 1 for ChatGPT or 2 for Claude."
    except Exception as e:
        error_message = f"Error calling AI API (provider {AI_PROVIDER}): {e}"
        print(error_message)
        logger.error(error_message)
        return error_message



def call_chatgpt_api(prompt: str) -> str:

    try:
        response = openai_client.chat.completions.create(
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


def call_claude_api(prompt: str) -> str:
    """Call Claude API"""
    try:
        response = anthropic_client.messages.create(
            # model="claude-3-5-sonnet-20241022",
            # model="claude-opus-4-20250514",
            model="claude-opus-4-20250514",
            max_tokens=4000,
            temperature=0.3,
            system="You are an SAP license optimization assistant.",
            messages=[
                {"role": "user", "content": prompt}
            ]
        )

        if response.content:
            return response.content[0].text.strip()
        else:
            return "Error: No response from Claude API."

    except Exception as e:
        error_message = f"Error calling Claude API: {e}"
        print(error_message)
        return error_message


def call_chatgpt_api_legacy(prompt: str) -> str:
    """Legacy function for backward compatibility"""
    return call_chatgpt_api(prompt)
