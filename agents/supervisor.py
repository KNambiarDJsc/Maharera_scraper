# agents/supervisor.py
import os
import logging
from dotenv import load_dotenv

from langchain_openai import ChatOpenAI
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

from agents.search_agent import search_tool
from agents.scraper_agent import scrape_project_tool

# Load .env (so OPENAI_API_KEY is picked up)
load_dotenv()

logger = logging.getLogger("maharera.supervisor")


# -------------------------
# SYSTEM PROMPT
# -------------------------
SUPERVISOR_SYSTEM_PROMPT = """
You are the SUPERVISOR agent for the MahaRERA Intelligence System.

Your responsibilities:

1. Understand the user query.
2. Decide whether to:
   - Use SCRAPER TOOL directly if user provides a valid RERA ID (format: Pxxxxx...),
   - Or use SEARCH TOOL when user gives building name / developer name / locality etc.
3. Route correctly:
   • Tool: maharera_search → when user does NOT give a registration number.
   • Tool: maharera_scrape → when user gives a valid registration number.
4. Output ONLY the final structured JSON.
5. Do NOT hallucinate RERA numbers.
6. If search returns NOT_FOUND → ask user for more details.
"""

# -------------------------
# PROMPT
# -------------------------
PROMPT = ChatPromptTemplate.from_messages(
    [
        ("system", SUPERVISOR_SYSTEM_PROMPT),
        MessagesPlaceholder("chat_history", optional=True),
        ("human", "{input}"),
        MessagesPlaceholder("agent_scratchpad"),
    ]
)


# -------------------------
# CREATE SUPERVISOR
# -------------------------
def create_supervisor_agent():
    """
    Creates the main supervisor agent (controller agent).
    It uses OpenAI function-calling model to invoke search/scraper tools.
    """

    llm = ChatOpenAI(
        model="gpt-4o-mini",   # perfect routing model
        temperature=0,
    )

    tools = [
        search_tool,
        scrape_project_tool
    ]

    # Build OpenAI Tools Agent
    agent = create_openai_tools_agent(
        llm=llm,
        tools=tools,
        prompt=PROMPT,
    )

    # Agent executor handles loops + tool calls
    executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True,
    )

    return executor
