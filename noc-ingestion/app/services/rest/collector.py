import time
import asyncio
from typing import Optional, Dict, Any, List
import httpx

from app.core.exceptions import RESTCollectorError, RESTTimeoutError
from app.core.logger import log_event, logger
from app.schemas.rest_schema import RESTCollectorRequest, RESTCollectorResult, TargetSystem
from app.services.storage.minio.client import MinIOService, get_minio_service


class RESTCollectorService:
    """Service to collect data from REST APIs with pagination, auth, timeouts & retries."""

    def __init__(self, minio_service: Optional[MinIOService] = None):
        self.minio_service = minio_service or get_minio_service()

    async def fetch_page_with_retry(
        self,
        client: httpx.AsyncClient,
        url: str,
        headers: Dict[str, str],
        params: Dict[str, Any],
        max_retries: int = 3,
        backoff_factor: float = 1.0,
    ) -> Dict[str, Any]:
        """Fetches a single REST page with exponential backoff retries."""
        last_exception = None

        for attempt in range(1, max_retries + 1):
            try:
                response = await client.get(url, headers=headers, params=params)
                response.raise_for_status()
                return response.json()
            except httpx.TimeoutException as te:
                last_exception = RESTTimeoutError(f"REST API call timed out on attempt {attempt}/{max_retries}: {te}")
                logger.warning(f"REST API timeout attempt {attempt}/{max_retries} for URL {url}")
            except httpx.HTTPStatusError as hse:
                last_exception = RESTCollectorError(
                    f"HTTP error {hse.response.status_code} on attempt {attempt}/{max_retries}: {hse.response.text}"
                )
                if hse.response.status_code < 500 and hse.response.status_code != 429:
                    # Client errors (4xx except 429) do not retry
                    raise last_exception
            except Exception as e:
                last_exception = RESTCollectorError(f"Unexpected REST error on attempt {attempt}/{max_retries}: {e}")

            if attempt < max_retries:
                sleep_time = backoff_factor * (2 ** (attempt - 1))
                await asyncio.sleep(sleep_time)

        raise last_exception or RESTCollectorError(f"Failed to fetch {url} after {max_retries} attempts.")

    async def collect_and_store(self, req: RESTCollectorRequest) -> RESTCollectorResult:
        """Executes full collection process across paginated endpoints and stores payload in MinIO."""
        start_time = time.time()

        log_event(
            event_type="API Started",
            status="IN_PROGRESS",
            details={"target_system": req.target_system.value, "url": req.url},
        )

        headers = req.headers or {}
        if req.auth_token:
            headers["Authorization"] = f"Bearer {req.auth_token}"
        headers.setdefault("Accept", "application/json")

        collected_records: List[Dict[str, Any]] = []
        pages_fetched = 0

        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            for page in range(1, req.max_pages + 1):
                params = dict(req.query_params or {})
                params["page"] = page
                params["limit"] = req.page_size

                try:
                    page_data = await self.fetch_page_with_retry(
                        client=client,
                        url=req.url,
                        headers=headers,
                        params=params,
                        max_retries=3,
                    )
                    pages_fetched += 1

                    # Standardize payload extraction (ServiceNow uses 'result', Trend Micro uses 'data', etc.)
                    if isinstance(page_data, list):
                        records = page_data
                    elif isinstance(page_data, dict):
                        records = page_data.get("result") or page_data.get("data") or page_data.get("items") or [page_data]
                    else:
                        records = []

                    collected_records.extend(records)

                    # Stop if no records returned on page
                    if not records:
                        break

                except Exception as e:
                    log_event(
                        event_type="API Error",
                        status="FAILED",
                        details={"target_system": req.target_system.value, "page": page, "error": str(e)},
                        level="ERROR",
                    )
                    # If first page failed, raise; otherwise store what we got
                    if page == 1:
                        raise e
                    break

        execution_time = round(time.time() - start_time, 3)

        # Store complete JSON response into MinIO raw/rest/
        payload_to_store = {
            "target_system": req.target_system.value,
            "url": req.url,
            "pages_fetched": pages_fetched,
            "total_records": len(collected_records),
            "collected_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "data": collected_records,
        }

        filename_prefix = f"rest_{req.target_system.value.lower().replace(' ', '_')}"
        minio_path = await self.minio_service.async_upload_json(
            data_dict=payload_to_store,
            category="rest",
            filename_prefix=filename_prefix,
        )

        log_event(
            event_type="API Success",
            status="SUCCESS",
            details={
                "target_system": req.target_system.value,
                "pages_fetched": pages_fetched,
                "records_fetched": len(collected_records),
                "minio_path": minio_path,
                "execution_time_seconds": execution_time,
            },
        )

        return RESTCollectorResult(
            target_system=req.target_system,
            url=req.url,
            pages_fetched=pages_fetched,
            records_fetched=len(collected_records),
            minio_path=minio_path,
            status="SUCCESS",
            execution_time_seconds=execution_time,
        )
