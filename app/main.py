from typing import Generator
from fastapi import FastAPI, File, UploadFile, Depends, WebSocket
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
import os

from integrations.Integrator_Wrapper import Integrator
from integrations.WebSocket import WebSocketHandler

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # This is for development only; specify your frontend origin in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
TEMP_FILE_STORAGE = "temp_files"  # Define a directory for temporary storage


@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    try:
        file_name = file.filename
        temp_file_path = os.path.join(TEMP_FILE_STORAGE, file_name)
        # Save file temporarily
        with open(temp_file_path, "wb") as temp_file:
            content = await file.read()
            temp_file.write(content)


        return {"file_name": file.filename, "status": "Columns check required", "action": "initiate_ws_connection"}
    except Exception as e:
        return {"file_name": file.filename, "status": f"An error occurred: {e}"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    WebSocketHandler.websocket = websocket
    await websocket.accept()
    try:
        file_name = await websocket.receive_text()
        temp_file_path = os.path.join(TEMP_FILE_STORAGE, file_name)
        integrator = Integrator(websocket, temp_file_path)
        await integrator.validate_file()
    except Exception as e:
        await websocket.send_text(f"An error occurred: {e}")
    finally:
        await websocket.close()
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
