#!/usr/bin/env python
"""
Google Spreadsheet MCP Server
A Model Context Protocol (MCP) server built with FastMCP for interacting with Google Sheets.
"""

import base64
import os
import sys
import logging
import contextvars
import asyncio
from typing import List, Dict, Any, Optional, Union
import json
from dataclasses import dataclass
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

# MCP imports
from mcp.server.fastmcp import FastMCP, Context
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.requests import Request as StarletteRequest
from starlette.routing import Mount, Route

# Google API imports
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import google.auth

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constants
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
CREDENTIALS_CONFIG = os.environ.get('CREDENTIALS_CONFIG')
TOKEN_PATH = os.environ.get('TOKEN_PATH', 'token.json')
CREDENTIALS_PATH = os.environ.get('CREDENTIALS_PATH', 'credentials.json')
SERVICE_ACCOUNT_PATH = os.environ.get('SERVICE_ACCOUNT_PATH', 'service_account.json')
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID', '')  # Working directory in Google Drive

# Session timeout configuration (in seconds)
# Can be overridden via environment variables
SSE_TIMEOUT = int(os.environ.get('SSE_TIMEOUT', '3600'))  # Default: 1 hour
SSE_KEEPALIVE_TIMEOUT = int(os.environ.get('SSE_KEEPALIVE_TIMEOUT', '30'))  # Default: 30 seconds
SSE_INACTIVITY_TIMEOUT = int(os.environ.get('SSE_INACTIVITY_TIMEOUT', '300'))  # Default: 5 minutes

# Context variable to store per-request authorization header
# Note: contextvars are automatically isolated per async context/task.
# Each SSE connection runs in its own async context, so multiple concurrent
# connections will each have their own isolated auth_header value.
auth_header_var: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar('auth_header', default=None)

# Context variables to store per-session Google API services
# These are built from the auth header for each SSE session
sheets_service_var: contextvars.ContextVar[Optional[Any]] = contextvars.ContextVar('sheets_service', default=None)
drive_service_var: contextvars.ContextVar[Optional[Any]] = contextvars.ContextVar('drive_service', default=None)

def get_auth_header() -> Optional[str]:
    """
    Get the authorization header from the current request context.
    
    This function retrieves the auth header that was captured when the SSE
    connection was established. The header is automatically scoped to the
    current async context, so each concurrent SSE session has its own
    isolated value.
    
    Returns:
        The authorization header string if present, None otherwise.
    """
    return auth_header_var.get()


def build_services_from_auth_header(auth_header: str) -> tuple[Any, Any]:
    """
    Build Google Sheets and Drive services from an authorization header.
    
    The auth header should contain a base64-encoded OAuth2 user credentials JSON.
    Expected format:
    {
        "token": "<access_token>",
        "refresh_token": "<refresh_token>",
        "token_uri": "https://oauth2.googleapis.com/token",
        "client_id": "<client_id>",
        "client_secret": "<client_secret>",
        "scopes": ["https://www.googleapis.com/auth/spreadsheets", ...]
    }
    
    Format: "Bearer <base64_encoded_json>"
    
    Args:
        auth_header: The authorization header containing base64-encoded OAuth2 credentials JSON
                    (e.g., "Bearer <base64_encoded_json>")
    
    Returns:
        Tuple of (sheets_service, drive_service)
    
    Raises:
        ValueError: If the auth header format is invalid or decoding fails
        Exception: If service creation fails
    """
    # Extract token from header (supports "Bearer <token>" or just "<token>")
    if auth_header.startswith("Bearer "):
        encoded_json = auth_header[7:]  # Remove "Bearer " prefix
    else:
        encoded_json = auth_header
    
    try:
        # Decode base64 to get the JSON string
        decoded_json = base64.b64decode(encoded_json).decode('utf-8')
    except Exception as e:
        raise ValueError(f"Failed to decode base64 credentials: {e}")
    
    try:
        # Parse the JSON to get OAuth2 user credentials info
        oauth_credentials_info = json.loads(decoded_json)
        logger.info(f"OAuth credentials info: {oauth_credentials_info}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse credentials JSON: {e}")
    
    # Validate required fields
    required_fields = ['token', 'refresh_token', 'token_uri', 'client_id', 'client_secret']
    missing_fields = [field for field in required_fields if field not in oauth_credentials_info]
    if missing_fields:
        raise ValueError(f"Missing required credential fields: {missing_fields}")
    
    try:
        # Create credentials from the OAuth2 user info
        # Use scopes from the credentials if provided, otherwise use default SCOPES
        creds_scopes = oauth_credentials_info.get('scopes', SCOPES)
        creds = Credentials.from_authorized_user_info(
            oauth_credentials_info,
            scopes=creds_scopes
        )
        
        logger.info("Built credentials from base64-encoded OAuth2 credentials JSON")
    except Exception as e:
        raise ValueError(f"Failed to create credentials from OAuth2 user info: {e}")
    
    # Build the services
    try:
        sheets_service = build('sheets', 'v4', credentials=creds)
        drive_service = build('drive', 'v3', credentials=creds)
        
        logger.info("Built Google API services from auth header")
        return sheets_service, drive_service
    except Exception as e:
        raise Exception(f"Failed to build Google API services: {e}")


def get_sheets_service(ctx: Optional[Context] = None) -> Any:
    """
    Get the sheets service from the current session contextvars.
    
    Args:
        ctx: Optional MCP Context (not used, kept for compatibility)
    
    Returns:
        The sheets service
    
    Raises:
        ValueError: If no service is available
    """
    service = sheets_service_var.get()
    if service is None:
        raise ValueError("No Google Sheets service available. Please provide an authorization header or configure default credentials.")
    return service


def get_drive_service(ctx: Optional[Context] = None) -> Any:
    """
    Get the drive service from the current session contextvars.
    
    Args:
        ctx: Optional MCP Context (not used, kept for compatibility)
    
    Returns:
        The drive service
    
    Raises:
        ValueError: If no service is available
    """
    service = drive_service_var.get()
    if service is None:
        raise ValueError("No Google Drive service available. Please provide an authorization header or configure default credentials.")
    return service


@dataclass
class SpreadsheetContext:
    """Context for Google Spreadsheet service"""
    sheets_service: Any
    drive_service: Any
    folder_id: Optional[str] = None


@asynccontextmanager
async def spreadsheet_lifespan(server: FastMCP) -> AsyncIterator[SpreadsheetContext]:
    # """Manage Google Spreadsheet API connection lifecycle"""
    # # Authenticate and build the service
    # creds = None

    # if CREDENTIALS_CONFIG:
    #     creds = service_account.Credentials.from_service_account_info(json.loads(base64.b64decode(CREDENTIALS_CONFIG)), scopes=SCOPES)
    
    # # Check for explicit service account authentication first (custom SERVICE_ACCOUNT_PATH)
    # if not creds and SERVICE_ACCOUNT_PATH and os.path.exists(SERVICE_ACCOUNT_PATH):
    #     try:
    #         # Regular service account authentication
    #         creds = service_account.Credentials.from_service_account_file(
    #             SERVICE_ACCOUNT_PATH,
    #             scopes=SCOPES
    #         )
    #         print("Using service account authentication")
    #         print(f"Working with Google Drive folder ID: {DRIVE_FOLDER_ID or 'Not specified'}")
    #     except Exception as e:
    #         print(f"Error using service account authentication: {e}")
    #         creds = None
    
    # # Fall back to OAuth flow if service account auth failed or not configured
    # if not creds:
    #     print("Trying OAuth authentication flow")
    #     if os.path.exists(TOKEN_PATH):
    #         with open(TOKEN_PATH, 'r') as token:
    #             creds = Credentials.from_authorized_user_info(json.load(token), SCOPES)
                
    #     # If credentials are not valid or don't exist, get new ones
    #     if not creds or not creds.valid:
    #         if creds and creds.expired and creds.refresh_token:
    #             try:
    #                 print("Attempting to refresh expired token...")
    #                 creds.refresh(Request())
    #                 print("Token refreshed successfully")
    #                 # Save the refreshed token
    #                 with open(TOKEN_PATH, 'w') as token:
    #                     token.write(creds.to_json())
    #             except Exception as refresh_error:
    #                 print(f"Token refresh failed: {refresh_error}")
    #                 print("Triggering reauthentication flow...")
    #                 creds = None  # Clear creds to trigger OAuth flow below

    #         # If refresh failed or creds don't exist, run OAuth flow
    #         if not creds:
    #             try:
    #                 flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
    #                 creds = flow.run_local_server(port=0)

    #                 # Save the credentials for the next run
    #                 with open(TOKEN_PATH, 'w') as token:
    #                     token.write(creds.to_json())
    #                 print("Successfully authenticated using OAuth flow")
    #             except Exception as e:
    #                 print(f"Error with OAuth flow: {e}")
    #                 creds = None
    
    # # Try Application Default Credentials if no creds thus far
    # # This will automatically check GOOGLE_APPLICATION_CREDENTIALS, gcloud auth, and metadata service
    # if not creds:
    #     try:
    #         print("Attempting to use Application Default Credentials (ADC)")
    #         print("ADC will check: GOOGLE_APPLICATION_CREDENTIALS, gcloud auth, and metadata service")
    #         creds, project = google.auth.default(
    #             scopes=SCOPES
    #         )
    #         print(f"Successfully authenticated using ADC for project: {project}")
    #     except Exception as e:
    #         print(f"Error using Application Default Credentials: {e}")
    #         raise Exception("All authentication methods failed. Please configure credentials.")
    
    # Build the services
    sheets_service =None #build('sheets', 'v4', credentials=creds)
    drive_service = None #build('drive', 'v3', credentials=creds)
    
    try:
        # Provide the service in the context
        yield SpreadsheetContext(
            sheets_service=sheets_service,
            drive_service=drive_service,
            folder_id=None
        )
    finally:
        # No explicit cleanup needed for Google APIs
        pass


class CustomFastMCP(FastMCP):
    """Custom FastMCP subclass that captures Authorization headers from GET/SSE requests."""
    
    async def run_sse_async(self, mount_path: str = "/") -> None:
        """Run the server using SSE transport with configurable timeouts."""
        import uvicorn
        starlette_app = self.sse_app()
        
        config = uvicorn.Config(
            starlette_app,
            host=self.settings.host,
            port=self.settings.port,
            log_level=self.settings.log_level.lower(),
            timeout_keep_alive=SSE_KEEPALIVE_TIMEOUT,
            timeout_graceful_shutdown=30,  # Grace period for shutdown
        )
        server = uvicorn.Server(config)
        logger.info(f"Starting SSE server with keepalive timeout: {SSE_KEEPALIVE_TIMEOUT}s, session timeout: {SSE_TIMEOUT}s")
        await server.serve()
    
    def sse_app(self) -> Starlette:
        """Return an instance of the SSE server app with header capture."""
        sse = SseServerTransport(self.settings.message_path)
        
        async def handle_sse(request: StarletteRequest) -> None:
            # IMPORTANT: This handler function runs in its own async task.
            # Contextvars are task-local and will be automatically cleaned up when this task ends,
            # even if we don't see the cleanup logs. The cleanup logs are for confirmation only.
            
            # Log connection attempt for debugging
            # Common reasons for multiple GET /sse calls:
            # 1. MCP client retry logic (if first connection fails)
            # 2. Client initialization (test connection + actual connection)
            # 3. Browser/client reconnection attempts
            # 4. CORS preflight (OPTIONS) followed by actual GET (but OPTIONS wouldn't hit this handler)
            client_info = f"{request.client.host if request.client else 'unknown'}:{request.client.port if request.client else 'unknown'}"
            logger.info(f"[CONNECT] ===== Handler function STARTED for client {client_info} =====")
            logger.info(f"[CONNECT] GET /sse connection attempt - Client: {client_info}, Has Auth: {bool(request.headers.get('authorization') or request.headers.get('Authorization'))}")
            
            # Capture Authorization header from GET/SSE request
            # Each SSE connection runs in its own async context, so the context variable
            # will be isolated per connection, allowing multiple concurrent sessions.
            auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
            if auth_header:
                # Log partial header for security (first 20 chars + indicator)
                logger.info(f"[CONNECT] Captured Authorization header from GET/SSE: {auth_header[:20]}...")
            else:
                logger.debug("[CONNECT] No Authorization header found in GET/SSE request")
            
            # Set up context variables BEFORE entering the SSE connection
            # This ensures they're available even if connection fails early
            if auth_header:
                auth_header_var.set(auth_header)
                try:
                    sheets_service, drive_service = build_services_from_auth_header(auth_header)
                    sheets_service_var.set(sheets_service)
                    drive_service_var.set(drive_service)
                    logger.info(f"[SETUP] Built per-session Google API services for client {client_info}")
                except Exception as e:
                    logger.error(f"[SETUP] Failed to build services from auth header for client {client_info}: {e}")
                    sheets_service_var.set(None)
                    drive_service_var.set(None)
            else:
                auth_header_var.set(None)
                sheets_service_var.set(None)
                drive_service_var.set(None)
            
            # Wrap the entire handler in try-finally to ensure cleanup
            # This is the outermost level - it should ALWAYS execute when the handler function exits
            # NOTE: If client disconnects abruptly, the handler might not exit immediately,
            # but contextvars are task-local and will be cleaned up when the task eventually ends
            try:
                logger.info(f"[CONNECT] Establishing SSE connection for client {client_info}...")
                async with sse.connect_sse(
                    request.scope,
                    request.receive,
                    request._send,  # type: ignore[reportPrivateUsage]
                ) as streams:
                    logger.info(f"[CONNECT] SSE connection established for client {client_info}, starting MCP server...")
                    try:
                        await self._mcp_server.run(
                            streams[0],
                            streams[1],
                            self._mcp_server.create_initialization_options(),
                        )
                        logger.info(f"[CONNECT] MCP server.run() completed normally for client {client_info}")
                    except asyncio.CancelledError:
                        logger.info(f"[DISCONNECT] MCP server.run() cancelled for client {client_info} (client disconnected)")
                        raise
                    except Exception as run_error:
                        logger.error(f"[ERROR] Error in MCP server.run() for client {client_info}: {run_error}", exc_info=True)
                        raise
            except asyncio.CancelledError:
                logger.info(f"[DISCONNECT] SSE connection cancelled for client {client_info}")
                raise
            except Exception as sse_error:
                logger.error(f"[ERROR] Error in SSE connection for client {client_info}: {sse_error}", exc_info=True)
                raise
            finally:
                # OUTERMOST finally block - should execute when handler function exits
                # IMPORTANT: This will execute when:
                # 1. The connection closes normally
                # 2. An exception occurs
                # 3. The handler function returns/exits for any reason
                # 
                # If this doesn't execute, it means the handler task is still running
                # (possibly waiting on a blocking operation). However, contextvars are
                # task-local and will be automatically cleaned up when the task ends.
                logger.info(f"[CLEANUP] ===== OUTER FINALLY: Handler function exiting for client {client_info} =====")
                logger.info(f"[CLEANUP] Cleaning up contextvars for client {client_info}")
                try:
                    # Verify what we're clearing
                    auth_before = auth_header_var.get()
                    sheets_before = sheets_service_var.get()
                    drive_before = drive_service_var.get()
                    
                    logger.info(f"[CLEANUP] Before cleanup - auth: {auth_before is not None}, sheets: {sheets_before is not None}, drive: {drive_before is not None}")
                    
                    auth_header_var.set(None)
                    sheets_service_var.set(None)
                    drive_service_var.set(None)
                    
                    # Verify they're cleared
                    auth_after = auth_header_var.get()
                    sheets_after = sheets_service_var.get()
                    drive_after = drive_service_var.get()
                    
                    logger.info(
                        f"[CLEANUP] After cleanup - auth: {auth_after is None}, sheets: {sheets_after is None}, drive: {drive_after is None}"
                    )
                    logger.info(
                        f"[CLEANUP] Contextvars cleared for client {client_info} - "
                        f"auth: {auth_before is not None}->{auth_after is None}, "
                        f"sheets: {sheets_before is not None}->{sheets_after is None}, "
                        f"drive: {drive_before is not None}->{drive_after is None}"
                    )
                    logger.info(f"[CLEANUP] ===== Cleanup completed for client {client_info} =====")
                except Exception as cleanup_error:
                    logger.error(f"[CLEANUP] ERROR during contextvar cleanup for client {client_info}: {cleanup_error}", exc_info=True)
        
        # POST message handler remains unchanged (no header capture)
        return Starlette(
            debug=self.settings.debug,
            routes=[
                Route(self.settings.sse_path, endpoint=handle_sse),
                Mount(self.settings.message_path, app=sse.handle_post_message),
            ],
        )


# Initialize the MCP server with lifespan management
# Resolve host/port from environment variables with flexible names
_resolved_host = os.environ.get('HOST') or os.environ.get('FASTMCP_HOST') or "0.0.0.0"
_resolved_port_str = os.environ.get('PORT') or os.environ.get('FASTMCP_PORT') or "8000"
try:
    _resolved_port = int(_resolved_port_str)
except ValueError:
    _resolved_port = 8000

# Initialize the MCP server with explicit host/port to ensure binding as configured
mcp = CustomFastMCP("Google Spreadsheet",
                     dependencies=["google-auth", "google-auth-oauthlib", "google-api-python-client"],
                     lifespan=spreadsheet_lifespan,
                     host=_resolved_host,
                     port=_resolved_port)


@mcp.tool()
def get_sheet_data(spreadsheet_id: str, 
                   sheet: str,
                   range: Optional[str] = None,
                   include_grid_data: bool = False,
                   ctx: Context = None) -> Dict[str, Any]:
    """
    Get data from a specific sheet in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: The name of the sheet
        range: Optional cell range in A1 notation (e.g., 'A1:C10'). If not provided, gets all data.
        include_grid_data: If True, includes cell formatting and other metadata in the response.
            Note: Setting this to True will significantly increase the response size and token usage
            when parsing the response, as it includes detailed cell formatting information.
            Default is False (returns values only, more efficient).
    
    Returns:
        Grid data structure with either full metadata or just values from Google Sheets API, depending on include_grid_data parameter
    """
    # Get service from contextvars with fallback to lifespan
    sheets_service = get_sheets_service(ctx)

    # Construct the range - keep original API behavior
    if range:
        full_range = f"{sheet}!{range}"
    else:
        full_range = sheet
    
    if include_grid_data:
        # Use full API to get all grid data including formatting
        result = sheets_service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            ranges=[full_range],
            includeGridData=True
        ).execute()
    else:
        # Use values API to get cell values only (more efficient)
        values_result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=full_range
        ).execute()
        
        # Format the response to match expected structure
        result = {
            'spreadsheetId': spreadsheet_id,
            'valueRanges': [{
                'range': full_range,
                'values': values_result.get('values', [])
            }]
        }

    return result

@mcp.tool()
def get_sheet_formulas(spreadsheet_id: str,
                       sheet: str,
                       range: Optional[str] = None,
                       ctx: Context = None) -> List[List[Any]]:
    """
    Get formulas from a specific sheet in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: The name of the sheet
        range: Optional cell range in A1 notation (e.g., 'A1:C10'). If not provided, gets all formulas from the sheet.
    
    Returns:
        A 2D array of the sheet formulas.
    """
    sheets_service = get_sheets_service(ctx)
    
    # Construct the range
    if range:
        full_range = f"{sheet}!{range}"
    else:
        full_range = sheet  # Get all formulas in the specified sheet
    
    # Call the Sheets API
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=full_range,
        valueRenderOption='FORMULA'  # Request formulas
    ).execute()
    
    # Get the formulas from the response
    formulas = result.get('values', [])
    return formulas

@mcp.tool()
def update_cells(spreadsheet_id: str,
                sheet: str,
                range: str,
                data: List[List[Any]],
                ctx: Context = None) -> Dict[str, Any]:
    """
    Update cells in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: The name of the sheet
        range: Cell range in A1 notation (e.g., 'A1:C10')
        data: 2D array of values to update
    
    Returns:
        Result of the update operation
    """
    sheets_service = get_sheets_service(ctx)
    
    # Construct the range
    full_range = f"{sheet}!{range}"
    
    # Prepare the value range object
    value_range_body = {
        'values': data
    }
    
    # Call the Sheets API to update values
    result = sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=full_range,
        valueInputOption='USER_ENTERED',
        body=value_range_body
    ).execute()
    
    return result


@mcp.tool()
def batch_update_cells(spreadsheet_id: str,
                       sheet: str,
                       ranges: Dict[str, List[List[Any]]],
                       ctx: Context = None) -> Dict[str, Any]:
    """
    Batch update multiple ranges in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: The name of the sheet
        ranges: Dictionary mapping range strings to 2D arrays of values
               e.g., {'A1:B2': [[1, 2], [3, 4]], 'D1:E2': [['a', 'b'], ['c', 'd']]}
    
    Returns:
        Result of the batch update operation
    """
    sheets_service = get_sheets_service(ctx)
    
    # Prepare the batch update request
    data = []
    for range_str, values in ranges.items():
        full_range = f"{sheet}!{range_str}"
        data.append({
            'range': full_range,
            'values': values
        })
    
    batch_body = {
        'valueInputOption': 'USER_ENTERED',
        'data': data
    }
    
    # Call the Sheets API to perform batch update
    result = sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=batch_body
    ).execute()
    
    return result


@mcp.tool()
def add_rows(spreadsheet_id: str,
             sheet: str,
             count: int,
             start_row: Optional[int] = None,
             ctx: Context = None) -> Dict[str, Any]:
    """
    Add rows to a sheet in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: The name of the sheet
        count: Number of rows to add
        start_row: 0-based row index to start adding. If not provided, adds at the beginning.
    
    Returns:
        Result of the operation
    """
    sheets_service = get_sheets_service(ctx)
    
    # Get sheet ID
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    
    for s in spreadsheet['sheets']:
        if s['properties']['title'] == sheet:
            sheet_id = s['properties']['sheetId']
            break
            
    if sheet_id is None:
        return {"error": f"Sheet '{sheet}' not found"}
    
    # Prepare the insert rows request
    request_body = {
        "requests": [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_row if start_row is not None else 0,
                        "endIndex": (start_row if start_row is not None else 0) + count
                    },
                    "inheritFromBefore": start_row is not None and start_row > 0
                }
            }
        ]
    }
    
    # Execute the request
    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=request_body
    ).execute()
    
    return result


@mcp.tool()
def add_columns(spreadsheet_id: str,
                sheet: str,
                count: int,
                start_column: Optional[int] = None,
                ctx: Context = None) -> Dict[str, Any]:
    """
    Add columns to a sheet in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: The name of the sheet
        count: Number of columns to add
        start_column: 0-based column index to start adding. If not provided, adds at the beginning.
    
    Returns:
        Result of the operation
    """
    sheets_service = get_sheets_service(ctx)
    
    # Get sheet ID
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    
    for s in spreadsheet['sheets']:
        if s['properties']['title'] == sheet:
            sheet_id = s['properties']['sheetId']
            break
            
    if sheet_id is None:
        return {"error": f"Sheet '{sheet}' not found"}
    
    # Prepare the insert columns request
    request_body = {
        "requests": [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": start_column if start_column is not None else 0,
                        "endIndex": (start_column if start_column is not None else 0) + count
                    },
                    "inheritFromBefore": start_column is not None and start_column > 0
                }
            }
        ]
    }
    
    # Execute the request
    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=request_body
    ).execute()
    
    return result


@mcp.tool()
def list_sheets(spreadsheet_id: str, ctx: Context = None) -> List[str]:
    """
    List all sheets in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
    
    Returns:
        List of sheet names
    """
    sheets_service = get_sheets_service(ctx)
    
    # Get spreadsheet metadata
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    
    # Extract sheet names
    sheet_names = [sheet['properties']['title'] for sheet in spreadsheet['sheets']]
    
    return sheet_names


@mcp.tool()
def copy_sheet(src_spreadsheet: str,
               src_sheet: str,
               dst_spreadsheet: str,
               dst_sheet: str,
               ctx: Context = None) -> Dict[str, Any]:
    """
    Copy a sheet from one spreadsheet to another.
    
    Args:
        src_spreadsheet: Source spreadsheet ID
        src_sheet: Source sheet name
        dst_spreadsheet: Destination spreadsheet ID
        dst_sheet: Destination sheet name
    
    Returns:
        Result of the operation
    """
    sheets_service = get_sheets_service(ctx)
    
    # Get source sheet ID
    src = sheets_service.spreadsheets().get(spreadsheetId=src_spreadsheet).execute()
    src_sheet_id = None
    
    for s in src['sheets']:
        if s['properties']['title'] == src_sheet:
            src_sheet_id = s['properties']['sheetId']
            break
            
    if src_sheet_id is None:
        return {"error": f"Source sheet '{src_sheet}' not found"}
    
    # Copy the sheet to destination spreadsheet
    copy_result = sheets_service.spreadsheets().sheets().copyTo(
        spreadsheetId=src_spreadsheet,
        sheetId=src_sheet_id,
        body={
            "destinationSpreadsheetId": dst_spreadsheet
        }
    ).execute()
    
    # If destination sheet name is different from the default copied name, rename it
    if 'title' in copy_result and copy_result['title'] != dst_sheet:
        # Get the ID of the newly copied sheet
        copy_sheet_id = copy_result['sheetId']
        
        # Rename the copied sheet
        rename_request = {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": copy_sheet_id,
                            "title": dst_sheet
                        },
                        "fields": "title"
                    }
                }
            ]
        }
        
        rename_result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=dst_spreadsheet,
            body=rename_request
        ).execute()
        
        return {
            "copy": copy_result,
            "rename": rename_result
        }
    
    return {"copy": copy_result}


@mcp.tool()
def rename_sheet(spreadsheet: str,
                 sheet: str,
                 new_name: str,
                 ctx: Context = None) -> Dict[str, Any]:
    """
    Rename a sheet in a Google Spreadsheet.
    
    Args:
        spreadsheet: Spreadsheet ID
        sheet: Current sheet name
        new_name: New sheet name
    
    Returns:
        Result of the operation
    """
    sheets_service = get_sheets_service(ctx)
    
    # Get sheet ID
    spreadsheet_data = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet).execute()
    sheet_id = None
    
    for s in spreadsheet_data['sheets']:
        if s['properties']['title'] == sheet:
            sheet_id = s['properties']['sheetId']
            break
            
    if sheet_id is None:
        return {"error": f"Sheet '{sheet}' not found"}
    
    # Prepare the rename request
    request_body = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "title": new_name
                    },
                    "fields": "title"
                }
            }
        ]
    }
    
    # Execute the request
    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet,
        body=request_body
    ).execute()
    
    return result


@mcp.tool()
def get_multiple_sheet_data(queries: List[Dict[str, str]], 
                            ctx: Context = None) -> List[Dict[str, Any]]:
    """
    Get data from multiple specific ranges in Google Spreadsheets.
    
    Args:
        queries: A list of dictionaries, each specifying a query. 
                 Each dictionary should have 'spreadsheet_id', 'sheet', and 'range' keys.
                 Example: [{'spreadsheet_id': 'abc', 'sheet': 'Sheet1', 'range': 'A1:B5'}, 
                           {'spreadsheet_id': 'xyz', 'sheet': 'Data', 'range': 'C1:C10'}]
    
    Returns:
        A list of dictionaries, each containing the original query parameters 
        and the fetched 'data' or an 'error'.
    """
    sheets_service = get_sheets_service(ctx)
    results = []
    
    for query in queries:
        spreadsheet_id = query.get('spreadsheet_id')
        sheet = query.get('sheet')
        range_str = query.get('range')
        
        if not all([spreadsheet_id, sheet, range_str]):
            results.append({**query, 'error': 'Missing required keys (spreadsheet_id, sheet, range)'})
            continue

        try:
            # Construct the range
            full_range = f"{sheet}!{range_str}"
            
            # Call the Sheets API
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=full_range
            ).execute()
            
            # Get the values from the response
            values = result.get('values', [])
            results.append({**query, 'data': values})

        except Exception as e:
            results.append({**query, 'error': str(e)})
            
    return results


@mcp.tool()
def get_multiple_spreadsheet_summary(spreadsheet_ids: List[str],
                                   rows_to_fetch: int = 5, 
                                   ctx: Context = None) -> List[Dict[str, Any]]:
    """
    Get a summary of multiple Google Spreadsheets, including sheet names, 
    headers, and the first few rows of data for each sheet.
    
    Args:
        spreadsheet_ids: A list of spreadsheet IDs to summarize.
        rows_to_fetch: The number of rows (including header) to fetch for the summary (default: 5).
    
    Returns:
        A list of dictionaries, each representing a spreadsheet summary. 
        Includes spreadsheet title, sheet summaries (title, headers, first rows), or an error.
    """
    sheets_service = get_sheets_service(ctx)
    summaries = []
    
    for spreadsheet_id in spreadsheet_ids:
        summary_data = {
            'spreadsheet_id': spreadsheet_id,
            'title': None,
            'sheets': [],
            'error': None
        }
        try:
            # Get spreadsheet metadata
            spreadsheet = sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                fields='properties.title,sheets(properties(title,sheetId))'
            ).execute()
            
            summary_data['title'] = spreadsheet.get('properties', {}).get('title', 'Unknown Title')
            
            sheet_summaries = []
            for sheet in spreadsheet.get('sheets', []):
                sheet_title = sheet.get('properties', {}).get('title')
                sheet_id = sheet.get('properties', {}).get('sheetId')
                sheet_summary = {
                    'title': sheet_title,
                    'sheet_id': sheet_id,
                    'headers': [],
                    'first_rows': [],
                    'error': None
                }
                
                if not sheet_title:
                    sheet_summary['error'] = 'Sheet title not found'
                    sheet_summaries.append(sheet_summary)
                    continue
                    
                try:
                    # Fetch the first few rows (e.g., A1:Z5)
                    # Adjust range if fewer rows are requested
                    max_row = max(1, rows_to_fetch) # Ensure at least 1 row is fetched
                    range_to_get = f"{sheet_title}!A1:{max_row}" # Fetch all columns up to max_row
                    
                    result = sheets_service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=range_to_get
                    ).execute()
                    
                    values = result.get('values', [])
                    
                    if values:
                        sheet_summary['headers'] = values[0]
                        if len(values) > 1:
                            sheet_summary['first_rows'] = values[1:max_row]
                    else:
                        # Handle empty sheets or sheets with less data than requested
                        sheet_summary['headers'] = []
                        sheet_summary['first_rows'] = []

                except Exception as sheet_e:
                    sheet_summary['error'] = f'Error fetching data for sheet {sheet_title}: {sheet_e}'
                
                sheet_summaries.append(sheet_summary)
            
            summary_data['sheets'] = sheet_summaries
            
        except Exception as e:
            summary_data['error'] = f'Error fetching spreadsheet {spreadsheet_id}: {e}'
            
        summaries.append(summary_data)
        
    return summaries


@mcp.resource("spreadsheet://{spreadsheet_id}/info")
def get_spreadsheet_info(spreadsheet_id: str) -> str:
    """
    Get basic information about a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet
    
    Returns:
        JSON string with spreadsheet information
    """
    # Get service from contextvars
    sheets_service = get_sheets_service()
    
    # Get spreadsheet metadata
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    
    # Extract relevant information
    info = {
        "title": spreadsheet.get('properties', {}).get('title', 'Unknown'),
        "sheets": [
            {
                "title": sheet['properties']['title'],
                "sheetId": sheet['properties']['sheetId'],
                "gridProperties": sheet['properties'].get('gridProperties', {})
            }
            for sheet in spreadsheet.get('sheets', [])
        ]
    }
    
    return json.dumps(info, indent=2)


@mcp.tool()
def create_spreadsheet(title: str, folder_id: Optional[str] = None, ctx: Context = None) -> Dict[str, Any]:
    """
    Create a new Google Spreadsheet.
    
    Args:
        title: The title of the new spreadsheet
        folder_id: Optional Google Drive folder ID where the spreadsheet should be created.
                  If not provided, uses the configured default folder or creates in root.
    
    Returns:
        Information about the newly created spreadsheet including its ID
    """
    drive_service = get_drive_service(ctx)
    # Use provided folder_id (no fallback to lifespan context)
    target_folder_id = folder_id

    # Create the spreadsheet
    file_body = {
        'name': title,
        'mimeType': 'application/vnd.google-apps.spreadsheet',
    }
    if target_folder_id:
        file_body['parents'] = [target_folder_id]
    
    spreadsheet = drive_service.files().create(
        supportsAllDrives=True,
        body=file_body,
        fields='id, name, parents'
    ).execute()

    spreadsheet_id = spreadsheet.get('id')
    parents = spreadsheet.get('parents')
    folder_info = f" in folder {target_folder_id}" if target_folder_id else " in root"
    print(f"Spreadsheet created with ID: {spreadsheet_id}{folder_info}")

    return {
        'spreadsheetId': spreadsheet_id,
        'title': spreadsheet.get('name', title),
        'folder': parents[0] if parents else 'root',
    }


@mcp.tool()
def create_sheet(spreadsheet_id: str, 
                title: str, 
                ctx: Context = None) -> Dict[str, Any]:
    """
    Create a new sheet tab in an existing Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet
        title: The title for the new sheet
    
    Returns:
        Information about the newly created sheet
    """
    sheets_service = get_sheets_service(ctx)
    
    # Define the add sheet request
    request_body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": title
                    }
                }
            }
        ]
    }
    
    # Execute the request
    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=request_body
    ).execute()
    
    # Extract the new sheet information
    new_sheet_props = result['replies'][0]['addSheet']['properties']
    
    return {
        'sheetId': new_sheet_props['sheetId'],
        'title': new_sheet_props['title'],
        'index': new_sheet_props.get('index'),
        'spreadsheetId': spreadsheet_id
    }


@mcp.tool()
def list_spreadsheets(folder_id: Optional[str] = None, ctx: Context = None) -> List[Dict[str, str]]:
    """
    List all spreadsheets in the specified Google Drive folder.
    If no folder is specified, uses the configured default folder or lists from 'My Drive'.
    
    Args:
        folder_id: Optional Google Drive folder ID to search in.
                  If not provided, uses the configured default folder or searches 'My Drive'.
    
    Returns:
        List of spreadsheets with their ID and title
    """
    # Get service from contextvars
    drive_service = get_drive_service(ctx)
    
    # Use provided folder_id (no fallback to lifespan context)
    target_folder_id = folder_id
    
    query = "mimeType='application/vnd.google-apps.spreadsheet'"
    
    # If a specific folder is provided or configured, search only in that folder
    if target_folder_id:
        query += f" and '{target_folder_id}' in parents"
        print(f"Searching for spreadsheets in folder: {target_folder_id}")
    else:
        print("Searching for spreadsheets in 'My Drive'")
    
    # List spreadsheets
    results = drive_service.files().list(
        q=query,
        spaces='drive',
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        fields='files(id, name)',
        orderBy='modifiedTime desc'
    ).execute()
    
    spreadsheets = results.get('files', [])
    
    return [{'id': sheet['id'], 'title': sheet['name']} for sheet in spreadsheets]


@mcp.tool()
def share_spreadsheet(spreadsheet_id: str, 
                      recipients: List[Dict[str, str]],
                      send_notification: bool = True,
                      ctx: Context = None) -> Dict[str, List[Dict[str, Any]]]:
    """
    Share a Google Spreadsheet with multiple users via email, assigning specific roles.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet to share.
        recipients: A list of dictionaries, each containing 'email_address' and 'role'.
                    The role should be one of: 'reader', 'commenter', 'writer'.
                    Example: [
                        {'email_address': 'user1@example.com', 'role': 'writer'},
                        {'email_address': 'user2@example.com', 'role': 'reader'}
                    ]
        send_notification: Whether to send a notification email to the users. Defaults to True.

    Returns:
        A dictionary containing lists of 'successes' and 'failures'. 
        Each item in the lists includes the email address and the outcome.
    """
    drive_service = get_drive_service(ctx)
    successes = []
    failures = []
    
    for recipient in recipients:
        email_address = recipient.get('email_address')
        role = recipient.get('role', 'writer') # Default to writer if role is missing for an entry
        
        if not email_address:
            failures.append({
                'email_address': None,
                'error': 'Missing email_address in recipient entry.'
            })
            continue
            
        if role not in ['reader', 'commenter', 'writer']:
             failures.append({
                'email_address': email_address,
                'error': f"Invalid role '{role}'. Must be 'reader', 'commenter', or 'writer'."
            })
             continue

        permission = {
            'type': 'user',
            'role': role,
            'emailAddress': email_address
        }
        
        try:
            result = drive_service.permissions().create(
                fileId=spreadsheet_id,
                body=permission,
                sendNotificationEmail=send_notification,
                fields='id'
            ).execute()
            successes.append({
                'email_address': email_address, 
                'role': role, 
                'permissionId': result.get('id')
            })
        except Exception as e:
            # Try to provide a more informative error message
            error_details = str(e)
            if hasattr(e, 'content'):
                try:
                    error_content = json.loads(e.content)
                    error_details = error_content.get('error', {}).get('message', error_details)
                except json.JSONDecodeError:
                    pass # Keep the original error string
            failures.append({
                'email_address': email_address,
                'error': f"Failed to share: {error_details}"
            })
            
    return {"successes": successes, "failures": failures}


@mcp.tool()
def list_folders(parent_folder_id: Optional[str] = None, ctx: Context = None) -> List[Dict[str, str]]:
    """
    List all folders in the specified Google Drive folder.
    If no parent folder is specified, lists folders from 'My Drive' root.
    
    Args:
        parent_folder_id: Optional Google Drive folder ID to search within.
                         If not provided, searches the root of 'My Drive'.
    
    Returns:
        List of folders with their ID, name, and parent information
    """
    drive_service = get_drive_service(ctx)
    
    query = "mimeType='application/vnd.google-apps.folder'"
    
    # If a specific parent folder is provided, search only within that folder
    if parent_folder_id:
        query += f" and '{parent_folder_id}' in parents"
        print(f"Searching for folders in parent folder: {parent_folder_id}")
    else:
        # Search in root of My Drive (folders that don't have any parent folders)
        query += " and 'root' in parents"
        print("Searching for folders in 'My Drive' root")
    
    # List folders
    results = drive_service.files().list(
        q=query,
        spaces='drive',
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        fields='files(id, name, parents)',
        orderBy='name'
    ).execute()
    
    folders = results.get('files', [])
    
    return [
        {
            'id': folder['id'], 
            'name': folder['name'],
            'parent': folder.get('parents', ['root'])[0] if folder.get('parents') else 'root'
        } 
        for folder in folders
    ]


@mcp.tool()
def batch_update(spreadsheet_id: str,
                 requests: List[Dict[str, Any]],
                 ctx: Context = None) -> Dict[str, Any]:
    """
    Execute a batch update on a Google Spreadsheet using the full batchUpdate endpoint.
    This provides access to all batchUpdate operations including adding sheets, updating properties,
    inserting/deleting dimensions, formatting, and more.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        requests: A list of request objects. Each request object can contain any valid batchUpdate operation.
                 Common operations include:
                 - addSheet: Add a new sheet
                 - updateSheetProperties: Update sheet properties (title, grid properties, etc.)
                 - insertDimension: Insert rows or columns
                 - deleteDimension: Delete rows or columns
                 - updateCells: Update cell values and formatting
                 - updateBorders: Update cell borders
                 - addConditionalFormatRule: Add conditional formatting
                 - deleteConditionalFormatRule: Remove conditional formatting
                 - updateDimensionProperties: Update row/column properties
                 - and many more...
                 
                 Example requests:
                 [
                     {
                         "addSheet": {
                             "properties": {
                                 "title": "New Sheet"
                             }
                         }
                     },
                     {
                         "updateSheetProperties": {
                             "properties": {
                                 "sheetId": 0,
                                 "title": "Renamed Sheet"
                             },
                             "fields": "title"
                         }
                     },
                     {
                         "insertDimension": {
                             "range": {
                                 "sheetId": 0,
                                 "dimension": "ROWS",
                                 "startIndex": 1,
                                 "endIndex": 3
                             }
                         }
                     }
                 ]
    
    Returns:
        Result of the batch update operation, including replies for each request
    """
    sheets_service = get_sheets_service(ctx)
    
    # Validate input
    if not requests:
        return {"error": "requests list cannot be empty"}
    
    if not all(isinstance(req, dict) for req in requests):
        return {"error": "Each request must be a dictionary"}
    
    # Prepare the batch update request body
    request_body = {
        "requests": requests
    }
    
    # Execute the batch update
    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=request_body
    ).execute()
    
    return result


def main():
    # Run the server
    transport = "stdio"
    for i, arg in enumerate(sys.argv):
        if arg == "--transport" and i + 1 < len(sys.argv):
            transport = sys.argv[i + 1]
            break

    mcp.run(transport=transport)
