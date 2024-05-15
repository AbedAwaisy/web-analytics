from typing import Generator
from fastapi import FastAPI, File, UploadFile, Depends, WebSocket
import pandas as pd
from fastapi.middleware.cors import CORSMiddleware
import uuid
import os

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
    await websocket.accept()
    # Expecting the first message to be the unique file ID
    file_name = await websocket.receive_text()
    temp_file_path = os.path.join(TEMP_FILE_STORAGE, file_name)

    # Step 1: Ask if the file has an Experiment column
    await websocket.send_text("Does this file have an Experiment column? (Yes/No)")
    has_experiment = await websocket.receive_text()
    if has_experiment.lower() == "yes":
        # Step 2: Prompt for Experiment column name
        await websocket.send_text("Please enter the Experiment column name:")
        experiment_column_name = await websocket.receive_text()

        # You can now process the file to check the columns count, etc.
        df = pd.read_csv(temp_file_path)
        columns_count = len(df.columns)
        rows_count = len(df)
        unique_values_sort = df[" סוג מיון"].unique().tolist()        # Step 3: Send processing message
        await websocket.send_text(f"Processing file '{temp_file_path}' with Experiment column '{experiment_column_name}'.")

        # Step 4: Send metadata and ask for confirmation to proceed
        await websocket.send_text(
            f"File '{temp_file_path}' has {columns_count} columns and {rows_count} rows and sort types {unique_values_sort}. Proceed with integration? (Yes/No)")
        proceed = await websocket.receive_text()

        if proceed.lower() == "yes":
            # Step 5: Integration success message
            await websocket.send_text("File inserted successfully.")
        else:
            await websocket.send_text("Integration cancelled by user.")
    else:
        await websocket.send_text("No Experiment column. Processing skipped.")
    #os.remove(temp_file_path)
    await websocket.close()
