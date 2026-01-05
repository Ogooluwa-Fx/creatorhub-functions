import azure.functions as func
import logging
import os
import json
import uuid
from datetime import datetime
from azure.cosmos import CosmosClient, exceptions
from azure.storage.blob import BlobServiceClient, ContentSettings

app = func.FunctionApp()

# -------------------------------------------------
# Configuration (DO NOT raise errors here)
# -------------------------------------------------
COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
COSMOS_KEY = os.getenv("COSMOS_KEY")
COSMOS_DATABASE = os.getenv("COSMOS_DATABASE")
COSMOS_CONTAINER = os.getenv("COSMOS_CONTAINER")

BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER", "uploads")

# -------------------------------------------------
# Clients (safe initialization)
# -------------------------------------------------
cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
database = cosmos_client.get_database_client(COSMOS_DATABASE)
container = database.get_container_client(COSMOS_CONTAINER)

blob_service = BlobServiceClient.from_connection_string(
    BLOB_CONNECTION_STRING
)
blob_container = blob_service.get_container_client(BLOB_CONTAINER_NAME)

# -------------------------------------------------
# CREATE ASSET (metadata only)
# POST /api/assets
# -------------------------------------------------
@app.function_name(name="create_asset")
@app.route(route="assets", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def create_asset(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()

        if "title" not in data or "blobUrl" not in data:
            return func.HttpResponse(
                "title and blobUrl are required",
                status_code=400
            )

        asset = {
            "id": str(uuid.uuid4()),
            "title": data["title"],
            "description": data.get("description", ""),
            "blobUrl": data["blobUrl"],
            "created_at": datetime.utcnow().isoformat()
        }

        container.create_item(asset)

        return func.HttpResponse(
            json.dumps(asset),
            status_code=201,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception("Create asset failed")
        return func.HttpResponse(str(e), status_code=500)

# -------------------------------------------------
# GET SINGLE ASSET
# GET /api/assets/{id}
# -------------------------------------------------
@app.function_name(name="get_asset")
@app.route(route="assets/{id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_asset(req: func.HttpRequest) -> func.HttpResponse:
    asset_id = req.route_params.get("id")

    try:
        asset = container.read_item(
            item=asset_id,
            partition_key=asset_id
        )

        return func.HttpResponse(
            json.dumps(asset),
            status_code=200,
            mimetype="application/json"
        )

    except exceptions.CosmosResourceNotFoundError:
        return func.HttpResponse("Asset not found", status_code=404)

# -------------------------------------------------
# LIST ASSETS
# GET /api/assets
# -------------------------------------------------
@app.function_name(name="list_assets")
@app.route(route="assets", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def list_assets(req: func.HttpRequest) -> func.HttpResponse:
    try:
        items = list(container.read_all_items())

        return func.HttpResponse(
            json.dumps(items),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception("List assets failed")
        return func.HttpResponse(str(e), status_code=500)

# -------------------------------------------------
# UPDATE ASSET
# PUT /api/assets/{id}
# -------------------------------------------------
@app.function_name(name="update_asset")
@app.route(route="assets/{id}", methods=["PUT"], auth_level=func.AuthLevel.ANONYMOUS)
def update_asset(req: func.HttpRequest) -> func.HttpResponse:
    asset_id = req.route_params.get("id")

    try:
        data = req.get_json()
        asset = container.read_item(
            item=asset_id,
            partition_key=asset_id
        )

        asset["title"] = data.get("title", asset["title"])
        asset["description"] = data.get("description", asset["description"])
        asset["blobUrl"] = data.get("blobUrl", asset["blobUrl"])
        asset["updated_at"] = datetime.utcnow().isoformat()

        container.replace_item(
            item=asset_id,
            body=asset
        )

        return func.HttpResponse(
            json.dumps(asset),
            status_code=200,
            mimetype="application/json"
        )

    except exceptions.CosmosResourceNotFoundError:
        return func.HttpResponse("Asset not found", status_code=404)

# -------------------------------------------------
# DELETE ASSET (Blob + Cosmos)
# DELETE /api/assets/{id}
# -------------------------------------------------
@app.function_name(name="delete_asset")
@app.route(route="assets/{id}", methods=["DELETE"], auth_level=func.AuthLevel.ANONYMOUS)
def delete_asset(req: func.HttpRequest) -> func.HttpResponse:
    asset_id = req.route_params.get("id")

    try:
        asset = container.read_item(
            item=asset_id,
            partition_key=asset_id
        )

        blob_name = asset["blobUrl"].split("/")[-1]
        blob_container.delete_blob(blob_name)

        container.delete_item(
            item=asset_id,
            partition_key=asset_id
        )

        return func.HttpResponse(status_code=204)

    except exceptions.CosmosResourceNotFoundError:
        return func.HttpResponse("Asset not found", status_code=404)

# -------------------------------------------------
# UPLOAD FILE
# POST /api/upload
# -------------------------------------------------
@app.function_name(name="upload_file")
@app.route(route="upload", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def upload_file(req: func.HttpRequest) -> func.HttpResponse:
    try:
        file_bytes = req.get_body()
        if not file_bytes:
            return func.HttpResponse("Empty file", status_code=400)

        filename = f"{uuid.uuid4()}.bin"

        blob_client = blob_container.get_blob_client(filename)

        blob_client.upload_blob(
            file_bytes,
            overwrite=True,
            content_settings=ContentSettings(
                content_type=req.headers.get("Content-Type", "application/octet-stream")
            )
        )

        return func.HttpResponse(
            json.dumps({
                "filename": filename,
                "url": blob_client.url
            }),
            status_code=201,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception("Upload failed")
        return func.HttpResponse(str(e), status_code=500)
