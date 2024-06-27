import pandas as pd
from datetime import datetime
from collections import defaultdict
import sys
from integrations.Helpper_Functions import *
from integrations.WebSocket import *


class Validation_Meta:
    def __init__(self, df, sorting_gap=1000, sorting_gap_std=0.2, date='2016-01-01'):
        self.df = df
        self.n = len(self.df)
        self.columns = columns_mapper(self.df)
        self.date = datetime.strptime(date, '%Y-%m-%d').date()
        self.sorting_gap = sorting_gap + sorting_gap * sorting_gap_std

    async def validate_harvest_date(self):
        harvest_date = self.columns['תאריך קטיף']
        # Convert the date column to datetime dtype with only date component
        data = self.df.copy()
        data.loc[:, harvest_date] = pd.to_datetime(data[harvest_date], errors='coerce').dt.date

        # Check for invalid date values
        invalid_dates = data[data[harvest_date].isna()]
        if not invalid_dates.empty:
            indices = [i + 2 for i in list(invalid_dates.index)]
            await WebSocketHandler.send_message(
                f"{len(invalid_dates)} out of {self.n} Invalid date value(s) founded in the harvest date column ({harvest_date.strip()}). \n the invalid samples indices are (the indices as it appears in the Excel file): \n {indices} \n give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[data[harvest_date].notna()]
                self.n = len(self.df)
                if self.n == 0:
                    await WebSocketHandler.send_message(
                        "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
                    sys.exit(1)
            else:
                await WebSocketHandler.send_message(
                    f"Invalid date value(s) founded in the harvest date column ({harvest_date.strip()}).")
                sys.exit(1)

    async def validate_sorting_date(self):
        # Convert the date_column to datetime
        sorting_date = self.columns['תאריך מיון']
        data = self.df.copy()
        data.loc[:, sorting_date] = pd.to_datetime(data[sorting_date], errors='coerce')

        # data[sorting_date] = pd.to_datetime(self.df[sorting_date], errors='coerce')

        # Check for invalid dates
        invalid_dates = data[data[sorting_date].isna()]
        if not invalid_dates.empty:
            indices = [i + 2 for i in list(invalid_dates.index)]
            await WebSocketHandler.send_message(
                f"{len(invalid_dates)} out of {self.n} Invalid date(s) founded in the sorting date column ({sorting_date.strip()}). \n the invalid samples indices are (the indices as it appears in the Excel file): \n {indices} \n give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df[data[sorting_date].notna()]
                self.n = len(self.df)
                if self.n == 0:
                    await WebSocketHandler.send_message(
                        "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
                    sys.exit(1)
            else:
                await WebSocketHandler.send_message(
                    f"Invalid date value(s) founded in the harvest date column ({sorting_date.strip()}).")
                sys.exit(1)

    async def validate_date_gap(self):
        sorting_date = self.columns['תאריך מיון']
        harvest_date = self.columns['תאריך קטיף']

        data = self.df.copy()
        # Calculate the absolute difference in hours
        data.loc[:, 'difference_hours'] = (data[sorting_date] - pd.to_datetime(
            data[harvest_date])).dt.total_seconds() / 3600

        # Count the number of rows where the difference exceeds the specified max gap
        num_exceeding_max_gap = (data['difference_hours'] > self.sorting_gap).sum()

        if num_exceeding_max_gap > 0:
            await WebSocketHandler.send_message(
                f"{num_exceeding_max_gap} out of {self.n} Invalid date gaps value(s) between harvest date and sorting date have been founded, max gap is {self.sorting_gap} hours, give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = data[data['difference_hours'] <= self.sorting_gap]
                self.n = len(self.df)
                if self.n == 0:
                    await WebSocketHandler.send_message(
                        "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
                    sys.exit(1)
            else:
                invalid_date_gaps = data[data['difference_hours'] > self.sorting_gap]
                indices = [i + 2 for i in list(invalid_date_gaps.index)]
                await WebSocketHandler.send_message(
                    f'\n the invalid samples indices are (the indices as it appears in the Excel file): \n {indices}')
                sys.exit(1)

    async def validate_date_order(self):
        sorting_datetime_col = self.columns['תאריך מיון']
        harvest_date_col = self.columns['תאריך קטיף']

        # Create a copy of the DataFrame
        data = self.df.copy()

        # Convert the date columns to datetime, assuming dayfirst format
        data[sorting_datetime_col] = pd.to_datetime(data[sorting_datetime_col], dayfirst=True)
        data[harvest_date_col] = pd.to_datetime(data[harvest_date_col], dayfirst=True).dt.date

        # Identify rows where the sorting datetime is not greater than the harvest date
        invalid_rows_df = data[data[sorting_datetime_col] < data[harvest_date_col]]

        if not invalid_rows_df.empty:
            indices = [i + 2 for i in list(invalid_rows_df.index)]
            await WebSocketHandler.send_message(
                f"{len(invalid_rows_df)} out of {self.n} invalid date order(s) found where sorting datetime is not greater than harvest date. \n The invalid sample indices are (the indices as they appear in the Excel file): \n {indices} \n Give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                # Drop invalid rows from the copied DataFrame
                data = data[data[sorting_datetime_col] >= data[harvest_date_col]]
                self.n = len(data)

                if self.n == 0:
                    await WebSocketHandler.send_message(
                        "No valid rows found to integrate or you gave authority to drop all the rows.")
                    sys.exit(1)

                # Update the original DataFrame with the cleaned copy
                self.df = data
            else:
                await WebSocketHandler.send_message(f"Invalid date order value(s) founded.")
                sys.exit(1)

    async def validate_sampleID(self):
        sampleID = self.columns['מזהה דגימה']
        data = self.df.copy()

        # check dublicates inside the file
        duplicates = data[data.duplicated(subset=sampleID, keep=False)]
        n = len(duplicates) // 2
        if n != 0:
            indices = [i + 2 for i in list(duplicates.index)]
            await WebSocketHandler.send_message(
                f"{n} out of {self.n} Duplicate Sample IDs founded ({sampleID.strip()}). \n the invalid samples indices are (the indices as it appears in the Excel file): \n {indices} \n give me authority to drop those rows or fix the file and try again (Yes/No): ")
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = data.drop_duplicates(subset=sampleID, keep='first')
                self.n = len(self.df)
                if self.n == 0:
                    await WebSocketHandler.send_message(
                        "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
                    sys.exit(1)
            else:
                await WebSocketHandler.send_message('Duplicate Sample IDs founded ({sampleID.strip()}).')
                sys.exit(1)

        # check if sample id already exists in the dbms
        if not is_empty_dbs():
            dbms_ids = get_existing_ids()
            Existing_ids = []
            for ids in dbms_ids:
                if ids in list(data[sampleID]):
                    Existing_ids.append(ids)
            n = len(Existing_ids)

            if n != 0:
                await WebSocketHandler.send_message(
                    f"{n} out of {self.n} Sample IDs already Exists in the dbms. \n the existing sample ids are: \n {Existing_ids} \n  give me authority to drop those rows or fix the file and try again (Yes/No): ")
                authority = await WebSocketHandler.receive_message()

                if authority.lower() == 'yes':
                    self.df = data[~data[sampleID].isin(Existing_ids)]
                    self.n = len(self.df)
                    if self.n == 0:
                        await WebSocketHandler.send_message(
                            "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
                        sys.exit(1)
                else:
                    sampleID_range = get_id_range()
                    await WebSocketHandler.send_message(
                        f'\n Existing Samples IDs founded ({sampleID.strip()}), that is already exists in the dbms. \n valid range for sample ID you can use is less than {sampleID_range[0][0]} or more than {sampleID_range[0][1]}.')
                    sys.exit(1)

    async def validate_parcel_size(self):
        parcel_size = self.columns['גודל חלקה במר']
        parcel_id = self.columns['חלקה']
        none_count = self.df[parcel_size].isna().sum()

        if is_empty_dbs():
            # df.dropna(subset=[column_name], inplace=True)
            if (self.df[parcel_size] == 0).all() or none_count == self.n:
                await WebSocketHandler.send_message(
                    f'all values of parcel size column (גודל חלקה במר) is not valid, all Parcels size must not be 0 or empty. please fix the file and try again.')
                sys.exit(1)
            elif (self.df[parcel_size] == 0).any() or none_count > 0:
                await WebSocketHandler.send_message(
                    'The values of parcel size column (גודל חלקה במר) is not valid, some of the Parcels with a size of 0 or empty. please fix the file or give me authority to delete those rows (Yes/No): ')
                authority = await WebSocketHandler.receive_message()

                if authority.lower() == 'yes':
                    self.df = self.df.loc[self.df[parcel_size] != 0]
                    self.df = self.df[self.df[parcel_size].notnull()]
                    self.n = len(self.df)
                    if self.n == 0:
                        await WebSocketHandler.send_message(
                            "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
                        sys.exit(1)
                else:
                    await WebSocketHandler.send_message('please fix parcel size column and try again.')
                    sys.exit(1)

        else:
            column_names = [parcel_id, parcel_size]
            values_to_check = self.df[column_names].values.tolist()
            connection = connect()
            cursor = connection.cursor()
            cursor.execute("SELECT ParcelID, ParcelSize from ParcelMetaData")
            database_values = cursor.fetchall()
            cursor.close()
            connection.close()
            database_values = [tuple(row) for row in database_values]
            database_praces = [row[0] for row in database_values]

            # insure that the prace size in the dbs is the same with the new file based on the prace id.
            for prace, prace_size in database_values:
                self.df.loc[self.df[parcel_id] == prace, parcel_size] = prace_size

            values_not_in_database = [value for value in values_to_check if value[0] not in database_praces]
            not_valid = count_not_valid(values_not_in_database)

            if not_valid == self.n or none_count == self.n:
                await WebSocketHandler.send_message(
                    'all values of parcel size column (גודל חלקה במר) is not valid, all Parcels with a size of 0 or empty cell. please correct the values and try again.')
                sys.exit(1)
            elif not_valid > 0 or none_count > 0:
                await WebSocketHandler.send_message(
                    'The values of parcel size column (גודל חלקה במר) is not valid, some of the Parcels with a size of 0 or empty cell. please correct the values or give me authority to delete those rows (Yes/No): ')
                authority = await WebSocketHandler.receive_message()

                if authority.lower() == 'yes':
                    self.df = self.df.loc[self.df[parcel_size] != 0]
                    self.df = self.df[self.df[parcel_size].notnull()]
                    self.n = len(self.df)
                    if self.n == 0:
                        await WebSocketHandler.send_message(
                            "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
                        sys.exit(1)
                else:
                    await WebSocketHandler.send_message('please fix parcel size column and try again.')
                    sys.exit(1)

    async def validate_exists_herb_name_info(self):
        knh = candidate_column('כנה', self.columns)
        rokev = candidate_column('רכב', self.columns)
        if ('זן' in self.columns.keys()) and self.df[self.columns['זן']].isnull().sum() != self.n:
            self.df[self.columns['זן']] = self.df[self.columns['זן']].astype(str)
            if (knh is None) and (rokev is None):
                self.df['HerbName'] = self.df.apply(lambda row: row[self.columns['זן']].strip(), axis=1)
            elif (knh is not None) and (rokev is None):
                self.df['HerbName'] = self.df.apply(
                    lambda row: row[self.columns['זן']].strip() + ' ' + row[self.columns[knh]].strip(), axis=1)
            elif (rokev is not None) and (knh is None):
                distribution_dict_rokev = self.df[self.columns[rokev]].value_counts().to_dict()
                if len(distribution_dict_rokev) == 2:
                    self.df['HerbName'] = self.df.apply(
                        lambda row: row[self.columns['זן']].strip() + ' ' 'לא מורכב' if ('ל' in row[
                            self.columns[rokev]]) and ('ל' in row[self.columns[rokev]]) and ('א' in row[
                            self.columns[rokev]]) else row[self.columns['זן']].strip() + ' ' + 'מורכב', axis=1)
                else:  # row[columns['זן']].strip() if row[columns[rokev[0]]].strip() == 'לא מורכב' else
                    self.df['HerbName'] = self.df.apply(
                        lambda row: row[self.columns[rokev]].strip() + ' ' + row[self.columns['זן']].strip(), axis=1)


        elif ('שם צמח' in self.columns.keys()) and self.df[self.columns['שם צמח']].isnull().sum() != self.n:
            self.df[self.columns['שם צמח']] = self.df[self.columns['שם צמח']].astype(str)
            if (knh is None) and (rokev is None):
                self.df['HerbName'] = self.df.apply(lambda row: row[self.columns['שם צמח']].strip(), axis=1)
            elif (knh is not None) and (rokev is None):
                self.df['HerbName'] = self.df.apply(
                    lambda row: row[self.columns['שם צמח']].strip() + ' ' + row[self.columns[knh]].strip(), axis=1)
            elif (knh is None) and (rokev is not None):
                distribution_dict_rokev = self.df[self.columns[rokev]].value_counts().to_dict()
                if len(distribution_dict_rokev) == 2:
                    self.df['HerbName'] = self.df.apply(
                        lambda row: row[self.columns['שם צמח']].strip() + ' ' 'לא מורכב' if ('ל' in row[
                            self.columns[rokev]]) and ('ל' in row[self.columns[rokev]]) and ('א' in row[
                            self.columns[rokev]]) else row[self.columns['שם צמח']].strip() + ' ' + 'מורכב', axis=1)
                else:
                    self.df['HerbName'] = self.df.apply(
                        lambda row: row[self.columns[rokev]].strip() + ' ' + row[self.columns['שם צמח']].strip(),
                        axis=1)

        elif (knh is not None) and (rokev is not None):
            self.df['HerbName'] = self.df.apply(
                lambda row: row[self.columns[rokev]].strip() + ' ' + row[self.columns[knh]].strip(), axis=1)

        elif (knh is not None) or (rokev is not None):
            if rokev is not None:
                self.df['HerbName'] = str(self.df[self.columns[rokev]]).strip()
            else:
                self.df['HerbName'] = str(self.df[self.columns[knh]]).strip()

        else:
            await WebSocketHandler.send_message(
                'missing herb name information, please add the missing information to the file and try again.')
            sys.exit(1)

        none_count = self.df['HerbName'].value_counts().get('nan', 0)
        if none_count > 0:
            missing = self.df[self.df['HerbName'] == 'nan']
            indices = [i + 2 for i in list(missing.index)]
            await WebSocketHandler.send_message(
                f'There is {none_count} out of {self.n} records with missing herb name information. \n The invalid samples indices with missing herb name information are (the indices as they appear in the Excel file): \n {indices} \n please correct the values or give me authority to delete those rows (Yes/No): ')
            authority = await WebSocketHandler.receive_message()

            if authority.lower() == 'yes':
                self.df = self.df.loc[self.df['HerbName'] != 'nan']
                self.n = len(self.df)
                if self.n == 0:
                    await WebSocketHandler.send_message(
                        "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
                    sys.exit(1)
            else:
                sys.exit(1)

    async def validate_herb_name_info_with_dbms(self, column_name='HerbName', table_name='ParcelMetaData'):
        # nothing to check dbms is empty
        if is_empty_dbs():
            return

        herbs_dbs = get_herbs_dbs()
        herbs_df = list(self.df['HerbName'].unique())
        herbs_dict = defaultdict(str)
        for herb_df in herbs_df:
            herb_tokens = herb_df.split()
            if len(herb_tokens) == 1:
                candidate_herbs = generate_levenshtein_distance_1_2(herb_df)
                for cand in candidate_herbs:
                    if cand in herbs_dbs:
                        await WebSocketHandler.send_message(
                            f'the system found that the herb name {herb_df} has a similar name in the dbs which is {cand}, is the two names indicate to the same herb name (Yes/No): ')
                        authority = await WebSocketHandler.receive_message()
                        if authority.lower() == 'yes':
                            self.df.loc[self.df['HerbName'] == herb_df, 'HerbName'] = cand
                            break

            else:
                flipped_herb_name_tokens = herb_tokens[::-1]
                flipped_herb_name_string = ' '.join(flipped_herb_name_tokens)

                candidate_herbs1 = generate_levenshtein_distance_1_2(herb_df)
                candidate_herbs2 = generate_levenshtein_distance_1_2(flipped_herb_name_string)
                for cand in candidate_herbs1.union(candidate_herbs2):
                    if cand in herbs_dbs:
                        await WebSocketHandler.send_message(
                            f'the system found that the herb name {herb_df} has a similar name in the dbs which is {cand}, is the two names indicate to the same herb name (Yes/No): ')
                        authority = await WebSocketHandler.receive_message()
                        if authority.lower() == 'yes':
                            self.df.loc[self.df['HerbName'] == herb_df, 'HerbName'] = cand
                            break