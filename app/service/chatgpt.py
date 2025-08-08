

import os
from pathlib import Path
from openai import OpenAI
from anthropic import Anthropic
from dotenv import load_dotenv

from app.core.logger import setup_logger, get_daily_log_filename

logger = setup_logger("app_logger")
dotenv_path = Path('./env')
load_dotenv()

# 1 = ChatGPT, 2 = Claude
AI_PROVIDER = 2

openai_client = OpenAI()
anthropic_client = Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
def call_ai_api(prompt: str) -> str:
    logger.info(f"Initiating AI call with prompt (first 100 chars): '{prompt[:100]}...'")
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
    logger.debug(f"Calling OpenAI API with model gpt-4o-mini and temperature=0.3")
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
            content = response.choices[0].message.content.strip()
            logger.info(f"Successfully received response from ChatGPT API. Response length: {len(content)}")
            logger.debug(f"Full response content: '{content}'")
            return response.choices[0].message.content.strip()
        else:
            logger.warning("OpenAI API returned a successful response, but without any choices.")
            return "Error: No response from OpenAI API."

    except Exception as e:
        error_message = f"Error calling OpenAI API: {e}"
        print(error_message)
        logger.error(error_message, exc_info=True)
        return error_message


def call_claude_api(prompt: str) -> str:
    """Call Claude API"""
    logger.debug(f"Calling Anthropic API with model claude-opus-4-20250514 and max_tokens=4000")

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
            content = response.content[0].text.strip()
            logger.info(f"Successfully received response from Claude API. Response length: {len(content)}")
            logger.debug(f"Full response content: '{content}'")
            return response.content[0].text.strip()
        else:
            logger.warning("Anthropic API returned a successful response, but without any content.")
            return "Error: No response from Claude API."

    except Exception as e:
        error_message = f"Error calling Claude API: {e}"
        print(error_message)
        logger.error(error_message, exc_info=True)
        return error_message


def call_chatgpt_api_legacy(prompt: str) -> str:
    """Legacy function for backward compatibility"""
    logger.warning("Using deprecated function 'call_chatgpt_api_legacy'. Consider updating calls to 'call_chatgpt_api'.")
    return call_chatgpt_api(prompt)
