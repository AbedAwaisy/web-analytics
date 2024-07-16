from integrations.Helpper_Functions import *
import math
import numpy as np
import sys
# websocket import, the file name is websocket
from integrations.WebSocket import *


class Validation_Yield:
    def __init__(self, df):
        self.columns = columns_mapper(df)
        df1 = update_single_harvest_number(df, self.columns['יצוא בודדים'], self.columns['יצוא בודדים מספר'])
        self.df = update_single_harvest_number(df1, self.columns['שחור פיטם'], self.columns['שחור פיטם מספר'])
        result_columns = ['ירוקים', 'סדוקים', 'שחור פיטם', 'אחרים']
        self.numeric_cols = [self.columns[c] for c in result_columns]
        self.n = len(self.df)
        self.id = self.columns['מזהה דגימה']
        self.biase = 1e-10

    async def validate_black_number(self):
        col = self.columns['שחור פיטם מספר']
        data_filtered = self.df[self.df[col] != 0]
        invalid_rows = data_filtered[data_filtered[col] == -1]
        invalid_rows1 = data_filtered[~data_filtered[col].apply(is_integer_whole_number)]
        data = data_filtered[data_filtered[col] != -1]
        data1 = data[data[col].apply(is_integer_whole_number)]

        if len(invalid_rows1) > 0:
            indices = [i + 2 for i in list(invalid_rows1.index)]
            await WebSocketHandler.send_message(
                f"there is {len(invalid_rows1)} out of {self.n} Invalid values in the Black Pit Defect Number column ({col.strip()}) the value of Black Pit Defect Number column has to be a whole number. \nThe invalid samples indices are (the indices as they appear in the Excel file): \n{indices} \n check the values and fix them or give me authority to go forword and drop those rows: ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows1[self.id])]
                self.n = len(self.df)

            else:
                await WebSocketHandler.send_message(f"Invalid value(s) found in the column {col.strip()}.")
                sys.exit(1)

        if len(invalid_rows) > 0:
            indices = [i + 2 for i in list(invalid_rows.index)]
            await WebSocketHandler.send_message(
                f"there is {len(invalid_rows)} out of {self.n} conflict values in the Black Pit Defect Number column ({col.strip()}) the value of Black Pit Defect column is not zero but Black Pit Defect number value is zero. \nThe invalid samples indices are (the indices as they appear in the Excel file): \n{indices} \n check the values and fix them or give me authority to go forword and drop those rows: ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows[self.id])]
                self.n = len(self.df)

            else:
                await WebSocketHandler.send_message(f"Invalid value(s) found in the column {col.strip()}.")
                sys.exit(1)

    async def validate_cluster_harvest(self, scale_thresh=1.3, unique_lower_mean=0.86, unique_lower_std=0.11):
        col = self.columns['יצוא אשכולות']

        # Check for values outside the specified range
        invalid_rows_df = self.df[self.df[col].apply(lambda x: math.log10(x + self.biase) > scale_thresh)]

        if not invalid_rows_df.empty:
            indices = [i + 2 for i in list(invalid_rows_df.index)]
            await WebSocketHandler.send_message(
                f"{len(invalid_rows_df)} out of {self.n} invalid weight scale found in the weight column ({col.strip()}). The log10 of the weight value must be less than {scale_thresh}. \nThe invalid samples indices are (the indices as they appear in the Excel file): \n{indices} \n give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows_df[self.id])]
                self.n = len(self.df)
            else:
                await WebSocketHandler.send_message(
                    f"Invalid weight value(s) found in the weight column ({col.strip()}).")
                sys.exit(1)

        data = self.df[self.df[col] != 0]
        n = len(data)
        number_of_unique_values = data[col].nunique()
        thresh = unique_lower_mean - 2*unique_lower_std
        if n != 0:
            if number_of_unique_values / n < thresh:
                await WebSocketHandler.send_message(
                    f"the system guesses that there is invalid values in the cluster harvest column ({col.strip()}), check the values and fix them or give me authority to go forword if you think that there is no problem with the column values (Yes/No): ")
                authority = await WebSocketHandler.receive_message()
                if authority.lower() != 'yes':
                    await WebSocketHandler.send_message(
                        f"Invalid single harvest value(s) founded in the cluster harvest column ({col.strip()}).")
                    sys.exit(1)

        else:
            await WebSocketHandler.send_message(
                f"alll the value(s) in the cluster harvest column ({col.strip()}) are invalid.")
            sys.exit(1)

    async def validate_single_harvest(self, scale_thresh=1.3, unique_lower_mean=0.78, unique_lower_std=0.14):
        col = self.columns['יצוא בודדים']

        # Check for values outside the specified range
        invalid_rows_df = self.df[self.df[col].apply(lambda x: math.log10(x + self.biase) > scale_thresh)]

        if not invalid_rows_df.empty:
            indices = [i + 2 for i in list(invalid_rows_df.index)]
            await WebSocketHandler.send_message(
                f"{len(invalid_rows_df)} out of {self.n} invalid weight scale found in the weight column ({col.strip()}). The log10 of the weight value must be less than {scale_thresh}. \nThe invalid samples indices are (the indices as they appear in the Excel file): \n{indices} \n give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows_df[self.id])]
                self.n = len(self.df)
            else:
                await WebSocketHandler.send_message(
                    f"Invalid weight value(s) found in the weight column ({col.strip()}).")
                sys.exit(1)

        data = self.df[self.df[col] != 0]
        n = len(data)
        number_of_unique_values = data[col].nunique()
        thresh = unique_lower_mean - 2*unique_lower_std
        if n > 0:
            if number_of_unique_values / n < thresh:
                await WebSocketHandler.send_message(
                    f"the system guesses that there is invalid values in the single harvest column ({col.strip()}), check the values and fix them or give me authority to go forword if you think that there is no problem with the column values (Yes/No): ")
                authority = await WebSocketHandler.receive_message()

                if authority.lower() != 'yes':
                    await WebSocketHandler.send_message(
                        f"Invalid single harvest value(s) founded in the single harvest column ({col.strip()}).")
                    sys.exit(1)

        else:
            await WebSocketHandler.send_message(
                f"alll the value(s) in the single harvest column ({col.strip()}) are invalid.")
            sys.exit(1)

    async def validate_numeric_cols(self, scale_thresh=1.3):
        # iterate over numeric columns
        for col in self.numeric_cols:
            # Check for values outside the specified range
            invalid_rows_df = self.df[self.df[col].apply(lambda x: math.log10(x + self.biase) > scale_thresh)]

            if not invalid_rows_df.empty:
                indices = [i + 2 for i in list(invalid_rows_df.index)]
                await WebSocketHandler.send_message(
                    f"{len(invalid_rows_df)} out of {self.n} invalid weight scale found in the weight column ({col.strip()}). The log10 of the weight value must be less than {scale_thresh}. \nThe invalid samples indices are (the indices as they appear in the Excel file): \n{indices} \n give me authority to drop those rows or fix the file and try again (Yes/No): ")
                authority = await WebSocketHandler.receive_message()
                if authority.lower() == 'yes':
                    self.df = self.df[~self.df[self.id].isin(invalid_rows_df[self.id])]
                    self.n = len(self.df)
                else:
                    await WebSocketHandler.send_message(
                        f"Invalid weight value(s) found in the weight column ({col.strip()}).")
                    sys.exit(1)

    async def validate_rows(self):
        c1 = self.columns['יצוא בודדים']
        c2 = self.columns['יצוא אשכולות']
        subset_columns = self.numeric_cols.copy()

        # Append two arguments to the new list
        subset_columns.append(c1)
        subset_columns.append(c2)

        mask = self.df[subset_columns].apply(count_identical_non_zero, axis=1)
        invalid_rows_df = self.df[~mask]

        if not invalid_rows_df.empty:
            indices = [i + 2 for i in list(invalid_rows_df.index)]
            await WebSocketHandler.send_message(
                f"{len(invalid_rows_df)} out of {self.n} invalid rows. The values are the same value in all or all most all result columns which is ambgious. \nThe invalid samples indices are (the indices as they appear in the Excel file): \n{indices} \n give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()
            if authority.lower() == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows_df[self.id])]
                self.n = len(self.df)
            else:
                await WebSocketHandler.send_message(f"Invalid value(s) found in the column {c.strip()}.")
                sys.exit(1)

    async def validate_single_harvest_number(self, thresh=0.25):
        col = self.columns['יצוא בודדים מספר']
        # self.df[col] = self.df[col].astype(float)
        data_filtered = self.df[self.df[col] != 0]
        invalid_rows = data_filtered[data_filtered[col] == -1]
        invalid_rows1 = data_filtered[~data_filtered[col].apply(is_integer_whole_number)]
        data = data_filtered[data_filtered[col] != -1]
        data1 = data[data[col].apply(is_integer_whole_number)]

        if len(invalid_rows1) > 0:
            indices = [i + 2 for i in list(invalid_rows1.index)]
            await WebSocketHandler.send_message(
                f"there is {len(invalid_rows1)} out of {self.n} Invalid values in the single harvest number column ({col.strip()}) the value of single harvest column has to be a whole number. \nThe invalid samples indices are (the indices as they appear in the Excel file): \n{indices} \n check the values and fix them or give me authority to go forword and drop those rows: ")
            authority = await WebSocketHandler.receive_message()
            if authority == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows1[self.id])]
                self.n = len(self.df)

            else:
                await WebSocketHandler.send_message(f"Invalid value(s) found in the column {col.strip()}.")
                sys.exit(1)

        if len(invalid_rows) > 0:
            indices = [i + 2 for i in list(invalid_rows.index)]
            await WebSocketHandler.send_message(
                f"there is {len(invalid_rows)} out of {self.n} conflict values in the single harvest number column ({col.strip()}) the value of single harvest column is not zero but single harvest number value is zero. \nThe invalid samples indices are (the indices as they appear in the Excel file): \n{indices} \n check the values and fix them or give me authority to go forword and drop those rows: ")
            authority = await WebSocketHandler.receive_message()
            if authority.lower() == 'yes':
                self.df = self.df[~self.df[self.id].isin(invalid_rows1[self.id])]
                self.n = len(self.df)

            else:
                await WebSocketHandler.send_message(f"Invalid value(s) found in the column {col.strip()}.")
                sys.exit(1)

        n = len(data1)
        number_of_unique_values = data1[col].nunique()

        if n > 0:
            if number_of_unique_values / n < thresh:
                await WebSocketHandler.send_message(
                    f"the system guesses that there is invalid values in the single harvest number column ({col.strip()}), check the values and fix them or give me authority to go forword if you think that there is no problem with the column values: ")
                authority = await WebSocketHandler.receive_message()
                if authority.lower() != 'yes':
                    await WebSocketHandler.send_message(
                        f"Invalid single harvest value(s) founded in the single harvest number column ({col.strip()}).")
                    sys.exit(1)





