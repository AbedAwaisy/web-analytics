from integrations.Helpper_Functions import *
import math
import numpy as np
import sys

# websocket import, the file name is websocket
from integrations.WebSocket import *


class Validation_Yield:
    def __init__(self, df):
        self.columns = columns_mapper(df)
        self.df = update_single_harvest_number(df, self.columns['יצוא בודדים'], self.columns['יצוא בודדים מספר'])
        self.n = len(self.df)
        self.biase = 1e-10

    async def validate_cluster_harvest(self, scale_thresh=1.3, unique_lower_mean=0.86, unique_lower_std=0.11):
        col = self.columns['יצוא אשכולות']

        # Check for values outside the specified range
        invalid_rows_df = self.df[self.df[col].apply(lambda x: math.log10(x + self.biase) > scale_thresh)]

        if not invalid_rows_df.empty:
            await WebSocketHandler.send_message(
                f"{len(invalid_rows_df)} out of {self.n} invalid weight scale found in the weight column ({col}). The log10 of the weight value must be less than {scale_thresh}, give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i + 2 for i in list(invalid_rows_df.index)]
                await WebSocketHandler.send_message(
                    f"\nThe invalid samples indices are (the indices as they appear in the Excel file): \n{indices}")
                sys.exit(1)

        data = self.df[self.df[col] != 0]
        n = len(data)
        number_of_unique_values = data[col].nunique()
        thresh = unique_lower_mean - unique_lower_std
        if n != 0:
            if number_of_unique_values / n < thresh:
                await WebSocketHandler.send_message(
                    f"the system guesses that there is invalid values in the cluster harvest column ({col}), check the values and fix them or give me authority to go forword if you think that there is no problem with the column values (Yes/No): ")
                authority = await WebSocketHandler.receive_message()
                if authority.lower() != 'yes':
                    await WebSocketHandler.send_message(f"Invalid single harvest value(s) founded in the cluster harvest column ({col}).")
                    sys.exit(1)

        else:
            await WebSocketHandler.send_message(f"alll the value(s) in the cluster harvest column ({col}) are invalid.")
            sys.exit(1)

    async def validate_single_harvest(self, scale_thresh=1.3, unique_lower_mean=0.78, unique_lower_std=0.14):
        col = self.columns['יצוא בודדים']

        # Check for values outside the specified range
        invalid_rows_df = self.df[self.df[col].apply(lambda x: math.log10(x + self.biase) > scale_thresh)]

        if not invalid_rows_df.empty:
            await WebSocketHandler.send_message(
                f"{len(invalid_rows_df)} out of {self.n} invalid weight scale found in the weight column ({col}). The log10 of the weight value must be less than {scale_thresh}, give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i + 2 for i in list(invalid_rows_df.index)]
                await WebSocketHandler.send_message(
                    f"\nThe invalid samples indices are (the indices as they appear in the Excel file): \n{indices}")
                sys.exit(1)

        data = self.df[self.df[col] != 0]
        n = len(data)
        number_of_unique_values = data[col].nunique()
        thresh = unique_lower_mean - unique_lower_std
        if n > 0:
            if number_of_unique_values / n < thresh:
                await WebSocketHandler.send_message(
                    f"the system guesses that there is invalid values in the single harvest column ({col}), check the values and fix them or give me authority to go forword if you think that there is no problem with the column values (Yes/No): ")
                authority = await WebSocketHandler.receive_message()

                if authority.lower() != 'yes':
                    await WebSocketHandler.send_message(f"Invalid single harvest value(s) founded in the single harvest column ({col}).")
                    sys.exit(1)

        else:
            await WebSocketHandler.send_message(f"alll the value(s) in the single harvest column ({col}) are invalid.")
            sys.exit(1)

    async def validate_single_harvest_number(self, thresh=0.5):
        col = self.columns['יצוא בודדים מספר']
        data_filtered = self.df[self.df[col] != 0]
        data = data_filtered[data_filtered[col] != -1]
        n = len(data)
        number_of_unique_values = data[col].nunique()
        if n > 0:
            if number_of_unique_values / n < thresh:
                await WebSocketHandler.send_message(
                    f"the system guesses that there is invalid values in the single harvest number column ({col}), check the values and fix them or give me authority to go forword if you think that there is no problem with the column values (Yes/No): ")
                authority = await WebSocketHandler.receive_message()

                if authority != 'yes':
                    await WebSocketHandler.send_message(
                        f"Invalid single harvest value(s) founded in the single harvest number column ({col}).")
                    sys.exit(1)
        else:
            await WebSocketHandler.send_message(f"alll the value(s) in the single harvest number column ({col}) are invalid.")
            sys.exit(1)

