class Validation_Meta:
    def __init__(self, df, sorting_gap=1000, sorting_gap_std=0.2, date='2010-01-01'):
        self.df = df
        self.n = len(self.df)
        self.columns = columns_mapper(self.df)
        self.date = datetime.strptime(date, '%Y-%m-%d').date()
        self.sorting_gap = sorting_gap + sorting_gap * sorting_gap_std

    def validate_harvest_date(self):
        harvest_date = self.columns['תאריך קטיף']
        # Convert the date column to datetime dtype with only date component
        # self.df[harvest_date] = pd.to_datetime(self.df[harvest_date], errors='coerce').dt.date
        self.df.loc[:, harvest_date] = pd.to_datetime(self.df[harvest_date], errors='coerce').dt.date

        # Check for invalid date values
        invalid_dates = self.df[self.df[harvest_date].isna()]
        if not invalid_dates.empty:
            authority = input(
                f"{len(invalid_dates)} out of {self.n} Invalid date value(s) founded in the harvest date column ({harvest_date}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[harvest_date].notna()]
                self.n = len(self.df)
                if self.n == 0:
                    raise ValueError(
                        "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
            else:
                indices = [i + 2 for i in list(invalid_dates.index)]
                print(
                    f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid date value(s) founded in the harvest date column ({harvest_date}).")

        valid_dates = self.df[(self.df[harvest_date] >= self.date)]
        invalid_dates = self.df[(self.df[harvest_date] < self.date)]
        if len(invalid_dates) != 0:
            authority = input(
                f"{len(invalid_dates)} out of {self.n} samples are recorded before {self.date}. founded in the harvest date column ({harvest_date}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[harvest_date].notna()]
                self.n = len(self.df)
                if self.n == 0:
                    raise ValueError(
                        "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
            else:
                indices = [i + 2 for i in list(invalid_dates.index)]
                print(f"\n the invalid samples indices are (the indices as it appears in the Excel file): \n {indices}")
                raise ValueError(f"records before {self.date} founded in the harvest date column ({harvest_date}).")

    def validate_sorting_date(self):
        # Convert the date_column to datetime
        sorting_date = self.columns['תאריך מיון']
        data = self.df.copy()
        self.df.loc[:, sorting_date] = pd.to_datetime(data[sorting_date], errors='coerce')
        # self.df.loc[:, sorting_date] = pd.to_datetime(self.df[sorting_date], errors='coerce')
        # self.df[sorting_date] = pd.to_datetime(self.df[sorting_date], errors='coerce')

        # Check for invalid dates
        invalid_dates = self.df[self.df[sorting_date].isna()]
        if not invalid_dates.empty:
            authority = input(
                f"{len(invalid_dates)} out of {self.n} Invalid date(s) founded in the sorting date column ({sorting_date}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[sorting_date].notna()]
                self.n = len(self.df)
                if self.n == 0:
                    raise ValueError(
                        "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
            else:
                indices = [i + 2 for i in list(invalid_dates.index)]
                print(f"\n the invalid samples indices are (the indices as it appears in the Excel file): \n {indices}")
                raise ValueError(f"Invalid date value(s) found in the harvest date column ({sorting_date}).")

    def validate_date_gap(self):
        sorting_date = self.columns['תאריך מיון']
        harvest_date = self.columns['תאריך קטיף']

        data = self.df.copy()
        # Calculate the absolute difference in hours
        data.loc[:, 'difference_hours'] = (data[sorting_date] - pd.to_datetime(
            data[harvest_date])).dt.total_seconds() / 3600

        # Count the number of rows where the difference exceeds the specified max gap
        num_exceeding_max_gap = (data['difference_hours'] > self.sorting_gap).sum()

        if num_exceeding_max_gap > 0:
            authority = input(
                f"{num_exceeding_max_gap} out of {self.n} Invalid date gaps value(s) between harvest date and sorting date have been founded, max gap is {self.sorting_gap} hours, give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = data[data['difference_hours'] <= self.sorting_gap]
                self.n = len(self.df)
                if self.n == 0:
                    raise ValueError(
                        "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
            else:
                invalid_date_gaps = data[data['difference_hours'] > self.sorting_gap]
                indices = [i + 2 for i in list(invalid_date_gaps.index)]
                print(f'\n the invalid samples indices are (the indices as it appears in the Excel file): \n {indices}')
                raise ValueError("Invalid date gaps value(s) between harvest date and sorting date have been founded")

    def validate_sampleID(self):
        sampleID = self.columns['מזהה דגימה']
        data = self.df.copy()

        # check dublicates inside the file
        duplicates = data[data.duplicated(subset=sampleID, keep=False)]
        n = len(duplicates) // 2
        if n != 0:
            authority = input(
                f"{n} out of {self.n} Duplicate Sample IDs founded ({sampleID}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = data.drop_duplicates(subset=sampleID, keep='first')
                self.n = len(self.df)
                if self.n == 0:
                    raise ValueError(
                        "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
            else:
                indices = [i + 2 for i in list(duplicates.index)]
                print(f'\n the invalid samples indices are (the indices as it appears in the Excel file): \n {indices}')
                raise ValueError(f"Duplicate Sample IDs founded ({sampleID}).")

        # check if sample id already exists in the dbms
        if not is_empty_dbs():
            dbms_ids = get_existing_ids()
            Existing_ids = []
            for ids in dbms_ids:
                if ids in list(data[sampleID]):
                    Existing_ids.append(ids)
            n = len(Existing_ids)
            if n != 0:
                authority = input(
                    f"{n} out of {self.n} Sample IDs already Exists in the dbms, give me authority to drop those rows or fix the file and try again: ")
                if authority == 'yes':
                    self.df = data[~data[sampleID].isin(Existing_ids)]
                    self.n = len(self.df)
                    if self.n == 0:
                        raise ValueError(
                            "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
                else:
                    sampleID_range = get_id_range()

                    print(
                        f'\n valid range for sample ID you can use is less than {sampleID_range[0][0]} or more than {sampleID_range[0][1]}. \n \n the samples IDs that are already exists in the dbms are: \n \n {Existing_ids}')
                    raise ValueError(f"Existing Samples IDs founded ({sampleID}), that is already exists in the dbms.")

    def validate_parcel_size(self):
        parcel_size = self.columns['גודל חלקה במר']
        parcel_id = self.columns['חלקה']
        none_count = self.df[parcel_size].isna().sum()

        if is_empty_dbs():
            # df.dropna(subset=[column_name], inplace=True)
            if (self.df[parcel_size] == 0).all() or none_count == self.n:
                raise ValueError(
                    f'all values of parcel size column (גודל חלקה במר) is not valid, all Parcels size must not be 0 or empty. please fix the file and try again.')
            elif (self.df[parcel_size] == 0).any() or none_count > 0:
                authority = input(
                    'The values of parcel size column (גודל חלקה במר) is not valid, some of the Parcels with a size of 0 or empty. please fix the file or give me authority to delete those rows: ')
                if authority == 'yes':
                    self.df = self.df.loc[self.df[parcel_size] != 0]
                    self.df = self.df[self.df[parcel_size].notnull()]
                    self.n = len(self.df)
                    if self.n == 0:
                        raise ValueError(
                            "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
                else:
                    raise ValueError('please fix parcel size column and try again.')

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
                raise ValueError(
                    'all values of parcel size column (גודל חלקה במר) is not valid, all Parcels with a size of 0 or empty cell. please correct the values and try again.')
            elif not_valid > 0 or none_count > 0:
                authority = input(
                    'The values of parcel size column (גודל חלקה במר) is not valid, some of the Parcels with a size of 0 or empty cell. please correct the values or give me authority to delete those rows: ')
                if authority == 'yes':
                    self.df = self.df.loc[self.df[parcel_size] != 0]
                    self.df = self.df[self.df[parcel_size].notnull()]
                    self.n = len(self.df)
                    if self.n == 0:
                        raise ValueError(
                            "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
                else:
                    raise ValueError('please fix parcel size column and try again.')

    def validate_exists_herb_name_info(self):
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
            raise ValueError(
                'missing herb name information, please add the missing information to the file and try again.')

        none_count = self.df['HerbName'].isna().sum()
        if none_count > 0:
            authority = input(
                'There is {none_count} out of {self.n} records with missing herb name information, please correct the values or give me authority to delete those rows: ')
            if authority == 'yes':
                self.df = self.df.loc[self.df['HerbName'].notnull()]
                self.n = len(self.df)
                if self.n == 0:
                    raise ValueError(
                        "No valid rows founded to integrate or you gave the autherity to drop all the rows.")
            else:
                raise ValueError('please correct the values and try again.')

                # no need to do

    def validate_herb_name_info_with_dbms(self, column_name='HerbName', table_name='ParcelMetaData'):
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
                    if cand in herbs_df:
                        self.df.loc[self.df['HerbName'] == herb_df, 'HerbName'] = cand
                        break
            else:
                flipped_herb_name_tokens = herb_tokens[::-1]
                flipped_herb_name_string = ' '.join(flipped_herb_name_tokens)

                candidate_herbs1 = generate_levenshtein_distance_1_2(herb_df)
                candidate_herbs2 = generate_levenshtein_distance_1_2(flipped_herb_name_string)
                for cand in candidate_herbs1.union(candidate_herbs2):
                    if cand in herbs_df:
                        self.df.loc[self.df['HerbName'] == herb_df, 'HerbName'] = cand
                        break