import pandas as pd
from integrations.Helpper_Functions import *
import math
import numpy as np
import sys
from integrations.WebSocket import *


class Validation_Quality:
    def __init__(self, df):
        self.df = df
        self.columns = columns_mapper(self.df)
        self.n = len(self.df)
        self.biase = 1e-10
        self.id = self.columns['מזהה דגימה']

    async def validate_weight(self, lower_thresh=0.1, upper_thresh=3.2):
        col = self.columns['משקל weight']

        # Check for values outside the specified range
        invalid_lower_bound_df = self.df[self.df[col].apply(lambda x: math.log10(x + self.biase) < lower_thresh)]
        invalid_upper_bound_df = self.df[self.df[col].apply(lambda x: math.log10(x + self.biase) > upper_thresh)]

        invalid_rows_df = pd.concat([invalid_lower_bound_df, invalid_upper_bound_df]).drop_duplicates()

        if not invalid_rows_df.empty:
            indices = [i + 2 for i in list(invalid_rows_df.index)]
            await WebSocketHandler.send_message(
                f"{len(invalid_rows_df)} out of {self.n} invalid weight scale found in the weight column ({col.strip()}). The log10 of the weight value must be less than {upper_thresh} and greater than {lower_thresh}. \nThe invalid samples indices are (the indices as they appear in the Excel file): \n{indices} \n give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows_df[self.id])]
                self.n = len(self.df)
            else:
                await WebSocketHandler.send_message(
                    f"\n Invalid weight scale found in the weight column ({col.strip()})")
                sys.exit(1)

    async def validate_fruiet_number(self):
        col = self.columns['מספר פרות']
        invalid_rows_df = self.df[~self.df[col].apply(is_integer_whole_number)]
        if not invalid_rows_df.empty:
            indices = [i + 2 for i in list(invalid_rows_df.index)]
            await WebSocketHandler.send_message(
                f"{len(invalid_rows_df)} out of {self.n} invalid fruiet number found in the column ({col.strip()}). the value of fruiet number column has to be a whole number. \nThe invalid samples indices are (the indices as they appear in the Excel file): \n{indices} \n check the values and fix them or give me authority to go forword and drop those rows: ")
            authority = await WebSocketHandler.receive_message()
            if authority == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows_df[self.id])]
                self.n = len(self.df)
            else:
                await WebSocketHandler.send_message(
                    f"Invalid fruiet number value(s) found in the column ({col.strip()}).")
                sys.exit(1)

    async def validate_vine_freshness(self, low=0, up=5):
        col = self.columns['רעננות שדרהvine freshness']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]

        if not invalid_rows_df.empty:
            indices = [i + 2 for i in list(invalid_rows_df.index)]
            await WebSocketHandler.send_message(
                f"{len(invalid_rows_df)} out of {self.n} Invalid vine freshness value(s) founded in the vine freshness column ({col.strip()}). the values must be in range({low}, {up}). \n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices} \n give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows_df[self.id])]
                self.n = len(self.df)
            else:
                await WebSocketHandler.send_message(
                    f"\n Invalid vine freshness value(s) found in column ({col.strip()})")
                sys.exit(1)

    async def validate_vine_root(self, low=0, up=5):
        col = self.columns['רקבון שזרהvine rot']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            indices = [i + 2 for i in list(invalid_rows_df.index)]
            await WebSocketHandler.send_message(
                f"{len(invalid_rows_df)} out of {self.n} Invalid vine root value(s) founded in the vine root column ({col.strip()}). the values must be in range({low}, {up}). \n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices} \n give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows_df[self.id])]
                self.n = len(self.df)
            else:
                await WebSocketHandler.send_message(f"\n Invalid vine root value(s) found in column ({col.strip()})")
                sys.exit(1)

    async def validate_shade(self, low=0, up=5):
        col = self.columns['גווןshade']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            indices = [i + 2 for i in list(invalid_rows_df.index)]
            await WebSocketHandler.send_message(
                f"{len(invalid_rows_df)} out of {self.n} Invalid shade value(s) founded in the shade column ({col.strip()}). the values must be in range({low}, {up}). \n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices} \n give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows_df[self.id])]
                self.n = len(self.df)
            else:
                await WebSocketHandler.send_message(f"\n Invalid shade value(s) found in column ({col.strip()})")
                sys.exit(1)

    async def validate_general_appereance(self, low=0, up=5):
        col = self.columns['מראה כלליgeneral appearance']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            indices = [i + 2 for i in list(invalid_rows_df.index)]
            await WebSocketHandler.send_message(
                f"{len(invalid_rows_df)} out of {self.n} Invalid general appereance value(s) founded in the general appereance column ({col.strip()}). the values must be in range({low}, {up}). \n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices} \n give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows_df[self.id])]
                self.n = len(self.df)
            else:
                await WebSocketHandler.send_message(
                    f"\n Invalid general appereance value(s) found in column ({col.strip()})")
                sys.exit(1)

    async def validate_color_virus(self, low=0, up=5):
        col = self.columns['וירוס צבעcolor virus']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            indices = [i + 2 for i in list(invalid_rows_df.index)]
            await WebSocketHandler.send_message(
                f"{len(invalid_rows_df)} out of {self.n} color virus value(s) founded in the color virus column ({col.strip()}). the values must be in range({low}, {up}). \n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices} \n give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows_df[self.id])]
                self.n = len(self.df)
            else:
                await WebSocketHandler.send_message(f"\n Invalid color virus value(s) found in column ({col.strip()})")
                sys.exit(1)

    async def validate_scratches_virus(self, low=0, up=5):
        col = self.columns['וירוס שריטותscratches virus']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            indices = [i + 2 for i in list(invalid_rows_df.index)]
            await WebSocketHandler.send_message(
                f"{len(invalid_rows_df)} out of {self.n} scratches virus value(s) founded in the scratches virus column ({col.strip()}). the values must be in range({low}, {up}). \n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices} \n give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows_df[self.id])]
                self.n = len(self.df)
            else:
                await WebSocketHandler.send_message(
                    f"\n Invalid scratches virus value(s) found in column ({col.strip()})")
                sys.exit(1)