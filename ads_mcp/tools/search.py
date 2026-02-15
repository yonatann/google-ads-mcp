# Copyright 2025 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tools for exposing the API Search method to the MCP server."""

import json
from typing import Any, Dict, List
from ads_mcp.coordinator import mcp
import ads_mcp.utils as utils


def search(
    customer_id: str,
    fields: List[str],
    resource: str,
    conditions: List[str] = None,
    orderings: List[str] = None,
    limit: int | str = None,
) -> List[Dict[str, Any]]:
    """Fetches data from the Google Ads API using the search method

    Args:
        customer_id: The id of the customer
        fields: The fields to fetch
        resource: The resource to return fields from
        conditions: List of conditions to filter the data, combined using AND clauses
        orderings: How the data is ordered
        limit: The maximum number of rows to return

    """

    ga_service = utils.get_googleads_service("GoogleAdsService")

    query_parts = [f"SELECT {','.join(fields)} FROM {resource}"]

    if conditions:
        query_parts.append(f" WHERE {' AND '.join(conditions)}")

    if orderings:
        query_parts.append(f" ORDER BY {','.join(orderings)}")

    if limit:
        query_parts.append(f" LIMIT {limit}")

    query = "".join(query_parts)
    utils.logger.info(f"ads_mcp.search query {query}")

    query_result = ga_service.search_stream(
        customer_id=customer_id, query=query
    )

    final_output: List = []
    for batch in query_result:
        for row in batch.results:
            final_output.append(
                utils.format_output_row(row, batch.field_mask.paths)
            )
    return final_output


def _load_resources() -> List[Dict]:
    """Loads the gaql_resources.json file and returns parsed JSON."""
    try:
        with open(utils.get_gaql_resources_filepath(), "r") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        utils.logger.error(f"Failed to load gaql_resources.json: {e}")
        return []


def _search_tool_description() -> str:
    """Returns a compact description for the `search` tool."""
    resources = _load_resources()
    resource_names = [r["resource"] for r in resources]

    return f"""
{search.__doc__}

### Hints
    Language Grammar can be found at https://developers.google.com/google-ads/api/docs/query/grammar
    All resources and descriptions are found at https://developers.google.com/google-ads/api/fields/v21/overview

    For Conversion issues try looking in offline_conversion_upload_conversion_action_summary

### Hint for customer_id
    should be a string of numbers without punctuation
    if presented in the form 123-456-7890 remove the hyphens and use 1234567890

### Hints for Dates
    All dates should be in the form YYYY-MM-DD and must include the dashes (-)
    Date literals from the Grammar must NEVER be used
    Date ranges should be finite and must include a start and end date

### Hints for limits
    Requests to resource change_event must specify a LIMIT of less than or equal to 10000

### Hints for conversions questions
    https://developers.google.com/google-ads/api/docs/conversions/upload-summaries

### Hints for fields
    IMPORTANT: Before calling search, use the get_resource_fields tool to look up
    the selectable, filterable, and sortable fields for your target resource.
    All fields must come from that tool and be prefixed with the resource being searched.
    Wildcards and partial fields are not allowed.

### Available resources
    {', '.join(resource_names)}
"""


@mcp.tool()
def get_resource_fields(resource: str) -> Dict[str, Any]:
    """Returns the selectable, filterable, and sortable fields for a Google Ads API resource.

    Call this tool before using the search tool to discover valid field names.

    Args:
        resource: The resource name (e.g. 'campaign', 'ad_group', 'ad_group_ad').
                  Use the search tool description to see all available resource names.
    """
    resources = _load_resources()
    for r in resources:
        if r["resource"] == resource:
            return r

    # Try partial match
    matches = [r for r in resources if resource in r["resource"]]
    if matches:
        return {
            "error": f"Resource '{resource}' not found. Did you mean one of: {[r['resource'] for r in matches[:10]]}?"
        }

    return {
        "error": f"Resource '{resource}' not found. Use the search tool description to see available resource names."
    }


# The `search` tool requires a more complex description that's generated at
# runtime. Uses the `add_tool` method instead of an annnotation since `add_tool`
# provides the flexibility needed to generate the description while also
# including the `search` method's docstring.
mcp.add_tool(
    search,
    title="Fetches data from the Google Ads API using the search method",
    description=_search_tool_description(),
)
