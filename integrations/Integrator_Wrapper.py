from typing import Optional
from fastapi import WebSocket
import pandas as pd
import os

class Integrator:
    def __init__(self, websocket: WebSocket, file_path: str):
        self.websocket = websocket
        self.file_path = file_path
        self.df = pd.read_csv(file_path)

    async def send_message(self, message: str):
        await self.websocket.send_text(message)

    async def receive_message(self) -> str:
        return await self.websocket.receive_text()

    async def validate_file(self):
        await self.send_message("Does this file have an Experiment column? (Yes/No)")
        has_experiment = await self.receive_message()

        if has_experiment.lower() == "yes":
            await self.send_message("Please enter the Experiment column name:")
            experiment_column_name = await self.receive_message()

            columns_count = len(self.df.columns)
            rows_count = len(self.df)
            unique_values_sort = self.df["סוג מיון"].unique().tolist()

            await self.send_message(
                f"Processing file '{self.file_path}' with Experiment column '{experiment_column_name}'."
            )

            await self.send_message(
                f"File '{self.file_path}' has {columns_count} columns and {rows_count} rows and sort types {unique_values_sort}. Proceed with integration? (Yes/No)"
            )
            proceed = await self.receive_message()

            if proceed.lower() == "yes":
                await self.integrate_file()
            else:
                await self.send_message("Integration cancelled by user.")
        else:
            await self.send_message("No Experiment column. Processing skipped.")

    async def integrate_file(self):
        try:
            # Here, instantiate and run the necessary classes and methods to integrate the file.
            # For example, let's assume we have a `run_integration` method:
            self.run_integration()
            await self.send_message("File inserted successfully.")
        except Exception as e:
            await self.send_message(f"An error occurred during integration: {e}")

    def run_integration(self):
        # Implement the integration logic here
        # For example, create instances of other classes and call their methods
        pass
