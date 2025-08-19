# SQL Analyzer

A Python tool that analyzes SQL files to extract entity-attribute relationships and query summaries using OpenAI's GPT-4o-mini model.

## Features

-   **Query-aware splitting**: Intelligently splits SQL files into individual queries
-   **Entity-Attribute mapping**: Maps database tables/entities to their related columns
-   **Query summarization**: Provides detailed explanations of what each query does
-   **Per-file organization**: Results organized by source file
-   **LLM-powered analysis**: Uses OpenAI GPT-4o-mini for intelligent SQL parsing

## Setup

1. **Install dependencies**:

    ```bash
    pip install -r requirements.txt
    ```

2. **Set up OpenAI API key**:

    - Create a `.env` file in the project root
    - Add your OpenAI API key:
        ```
        OPENAI_API_KEY=your_api_key_here
        ```
    - Or set it as an environment variable:
        ```bash
        export OPENAI_API_KEY=your_api_key_here
        ```

3. **Prepare SQL files**:
    - Place your SQL files in the `./sql_files` directory
    - Files should have `.sql` extension

## Usage

Run the analyzer:

```bash
python main.py
```

## Output

The tool generates a `per_file_results.json` file in the `./sql_analysis_output` directory with the following structure:

```json
{
    "file_path": {
        "entity_attributes": {
            "users": ["id", "name", "email", "created_at"],
            "orders": ["id", "user_id", "total", "status"],
            "order_items": ["order_id", "product_id", "quantity"]
        },
        "queries": [
            {
                "query_id": 1,
                "sql": "original SQL query",
                "entity_attributes": {
                    "users": ["id", "name"],
                    "orders": ["user_id", "total"]
                },
                "summary": "detailed explanation of what this query does..."
            }
        ]
    }
}
```

## Project Structure

```
sql_analyser/
├── main.py                    # Main analysis script
├── requirements.txt           # Python dependencies
├── README.md                 # This file
├── sql_files/                # Input SQL files
│   └── *.sql
└── sql_analysis_output/      # Generated output
    └── per_file_results.json
```

## Dependencies

-   `openai>=1.0.0`: OpenAI API client
-   `python-dotenv>=1.0.0`: Environment variable management
-   `sqlparse>=0.4.0`: SQL parsing utilities

## Notes

-   The tool processes queries sequentially to avoid API rate limits
-   Each query analysis includes a 0.3-second delay between API calls
-   The LLM is instructed to be conservative and only report tables/columns that are actually present in the query
-   Results are aggregated per file, showing all entities and attributes used across all queries in that file
