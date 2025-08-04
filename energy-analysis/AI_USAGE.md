🧠 AI Usage Assistant Documentation
For: energy-analysis project
Purpose: Leverage AI tools like ChatGPT and Gemini to assist with development, debugging, and documentation of the weather-energy data pipeline.

🔧 Key Use Cases for AI
1. Data Pipeline Design & Refactoring
Ask AI to:

Review existing pipeline (src/pipeline.py) and suggest simplifications.

Optimize retry logic or batching for API requests.

2. Debugging & Error Resolution
Usage Example:
If you encounter a timeout or HTTP error like:

pgsql
Copy code
HTTPSConnectionPool(host='api.eia.gov', port=443): Read timed out.
Prompt:

“My request to EIA API is timing out. How can I handle this with retry logic and exponential backoff?”

3. Unit Test Assistance
Usage Example:

“Write Pytest unit tests for the function that merges weather and energy datasets.”