import pandas as pd
from datetime import datetime
from collections import defaultdict
from Helpper_Functions import *
import math
import mysql.connector
import numpy as np

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
        #self.df[harvest_date] = pd.to_datetime(self.df[harvest_date], errors='coerce').dt.date
        self.df.loc[:, harvest_date] = pd.to_datetime(self.df[harvest_date], errors='coerce').dt.date

        
        # Check for invalid date values
        invalid_dates = self.df[self.df[harvest_date].isna()]
        if not invalid_dates.empty:
            authority = input(f"{len(invalid_dates)} out of {self.n} Invalid date value(s) founded in the harvest date column ({harvest_date}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[harvest_date].notna()]
                self.n = len(self.df)
                if self.n == 0:
                    raise ValueError("No valid rows founded to integrate or you gave the autherity to drop all the rows.")
            else:
                indices = [i+2 for i in list(invalid_dates.index)]
                print(f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid date value(s) founded in the harvest date column ({harvest_date}).")
                
        valid_dates = self.df[(self.df[harvest_date] >= self.date)]
        invalid_dates = self.df[(self.df[harvest_date] < self.date)]
        if len(invalid_dates) != 0:
            authority = input(f"{len(invalid_dates)} out of {self.n} samples are recorded before {self.date}. founded in the harvest date column ({harvest_date}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[harvest_date].notna()]
                self.n = len(self.df)
                if self.n == 0:
                    raise ValueError("No valid rows founded to integrate or you gave the autherity to drop all the rows.")
            else:
                indices = [i+2 for i in list(invalid_dates.index)]
                print(f"\n the invalid samples indices are (the indices as it appears in the Excel file): \n {indices}")
                raise ValueError(f"records before {self.date} founded in the harvest date column ({harvest_date}).")
            
        
    def validate_sorting_date(self):
        # Convert the date_column to datetime
        sorting_date = self.columns['תאריך מיון']
        data = self.df.copy()
        self.df.loc[:, sorting_date] = pd.to_datetime(data[sorting_date], errors='coerce')
        # self.df.loc[:, sorting_date] = pd.to_datetime(self.df[sorting_date], errors='coerce')
        #self.df[sorting_date] = pd.to_datetime(self.df[sorting_date], errors='coerce')
        
        # Check for invalid dates
        invalid_dates = self.df[self.df[sorting_date].isna()]
        if not invalid_dates.empty:
            authority = input(f"{len(invalid_dates)} out of {self.n} Invalid date(s) founded in the sorting date column ({sorting_date}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[sorting_date].notna()]
                self.n = len(self.df)
                if self.n == 0:
                    raise ValueError("No valid rows founded to integrate or you gave the autherity to drop all the rows.")
            else:
                indices = [i+2 for i in list(invalid_dates.index)]
                print(f"\n the invalid samples indices are (the indices as it appears in the Excel file): \n {indices}")
                raise ValueError(f"Invalid date value(s) found in the harvest date column ({sorting_date}).")
                
        
    def validate_date_gap(self):
        sorting_date = self.columns['תאריך מיון']
        harvest_date = self.columns['תאריך קטיף']
        
        data = self.df.copy()
        # Calculate the absolute difference in hours
        data.loc[:, 'difference_hours'] = (data[sorting_date] - pd.to_datetime(data[harvest_date])).dt.total_seconds() / 3600
        
        # Count the number of rows where the difference exceeds the specified max gap
        num_exceeding_max_gap = (data['difference_hours'] > self.sorting_gap).sum()
        
        if num_exceeding_max_gap > 0:
            authority = input(f"{num_exceeding_max_gap} out of {self.n} Invalid date gaps value(s) between harvest date and sorting date have been founded, max gap is {self.sorting_gap} hours, give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = data[data['difference_hours'] <= self.sorting_gap]
                self.n = len(self.df)
                if self.n == 0:
                    raise ValueError("No valid rows founded to integrate or you gave the autherity to drop all the rows.")
            else:
                invalid_date_gaps = data[data['difference_hours'] > self.sorting_gap]
                indices = [i+2 for i in list(invalid_date_gaps.index)]
                print(f'\n the invalid samples indices are (the indices as it appears in the Excel file): \n {indices}')
                raise ValueError("Invalid date gaps value(s) between harvest date and sorting date have been founded")
                
    def validate_sampleID(self):
        sampleID = self.columns['מזהה דגימה']
        data = self.df.copy()
        
        # check dublicates inside the file
        duplicates = data[data.duplicated(subset=sampleID, keep=False)]
        n = len(duplicates) // 2
        if n != 0:
            authority = input(f"{n} out of {self.n} Duplicate Sample IDs founded ({sampleID}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = data.drop_duplicates(subset=sampleID, keep='first')
                self.n = len(self.df)
                if self.n == 0:
                    raise ValueError("No valid rows founded to integrate or you gave the autherity to drop all the rows.")
            else:
                indices = [i+2 for i in list(duplicates.index)]
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
                authority = input(f"{n} out of {self.n} Sample IDs already Exists in the dbms, give me authority to drop those rows or fix the file and try again: ")
                if authority == 'yes':
                    self.df = data[~data[sampleID].isin(Existing_ids)]
                    self.n = len(self.df)
                    if self.n == 0:
                        raise ValueError("No valid rows founded to integrate or you gave the autherity to drop all the rows.")
                else:
                    sampleID_range = get_id_range()
    
                    print(f'\n valid range for sample ID you can use is less than {sampleID_range[0][0]} or more than {sampleID_range[0][1]}. \n \n the samples IDs that are already exists in the dbms are: \n \n {Existing_ids}')
                    raise ValueError(f"Existing Samples IDs founded ({sampleID}), that is already exists in the dbms.")
                    
    def validate_parcel_size(self):
        parcel_size = self.columns['גודל חלקה במר']
        parcel_id = self.columns['חלקה']
        none_count = self.df[parcel_size].isna().sum()
        
        if is_empty_dbs():
            #df.dropna(subset=[column_name], inplace=True)
            if (self.df[parcel_size] == 0).all() or none_count == self.n:
              raise ValueError(f'all values of parcel size column (גודל חלקה במר) is not valid, all Parcels size must not be 0 or empty. please fix the file and try again.')
            elif (self.df[parcel_size] == 0).any() or none_count > 0:
              authority = input('The values of parcel size column (גודל חלקה במר) is not valid, some of the Parcels with a size of 0 or empty. please fix the file or give me authority to delete those rows: ')
              if authority == 'yes':
                self.df = self.df.loc[self.df[parcel_size] !=0]
                self.df = self.df[self.df[parcel_size].notnull()]
                self.n = len(self.df)
                if self.n == 0:
                    raise ValueError("No valid rows founded to integrate or you gave the autherity to drop all the rows.")
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
              raise ValueError('all values of parcel size column (גודל חלקה במר) is not valid, all Parcels with a size of 0 or empty cell. please correct the values and try again.')
            elif not_valid > 0 or none_count > 0:
              authority = input('The values of parcel size column (גודל חלקה במר) is not valid, some of the Parcels with a size of 0 or empty cell. please correct the values or give me authority to delete those rows: ')
              if authority == 'yes':
                self.df = self.df.loc[self.df[parcel_size] !=0]
                self.df = self.df[self.df[parcel_size].notnull()]
                self.n = len(self.df)
                if self.n == 0:
                    raise ValueError("No valid rows founded to integrate or you gave the autherity to drop all the rows.")
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
                self.df['HerbName'] = self.df.apply(lambda row: row[self.columns['זן']].strip() + ' ' + row[self.columns[knh]].strip(), axis=1)
            elif (rokev is not None) and (knh is None):
              distribution_dict_rokev = self.df[self.columns[rokev]].value_counts().to_dict()
              if len(distribution_dict_rokev) == 2:
                self.df['HerbName'] = self.df.apply(lambda row: row[self.columns['זן']].strip() + ' ' 'לא מורכב' if ('ל' in row[self.columns[rokev]]) and ('ל' in row[self.columns[rokev]]) and ('א' in row[self.columns[rokev]]) else row[self.columns['זן']].strip() + ' ' + 'מורכב', axis=1)
              else: #row[columns['זן']].strip() if row[columns[rokev[0]]].strip() == 'לא מורכב' else
                self.df['HerbName'] = self.df.apply(lambda row: row[self.columns[rokev]].strip() + ' ' + row[self.columns['זן']].strip(), axis=1)
            

        elif ('שם צמח' in self.columns.keys()) and self.df[self.columns['שם צמח']].isnull().sum() != self.n:
          self.df[self.columns['שם צמח']] = self.df[self.columns['שם צמח']].astype(str)
          if (knh is None) and (rokev is None):
            self.df['HerbName'] = self.df.apply(lambda row: row[self.columns['שם צמח']].strip(), axis=1)
          elif (knh is not None) and (rokev is None):
            self.df['HerbName'] = self.df.apply(lambda row: row[self.columns['שם צמח']].strip() + ' ' + row[self.columns[knh]].strip(), axis=1)
          elif (knh is None) and (rokev is not None):
            distribution_dict_rokev = self.df[self.columns[rokev]].value_counts().to_dict()
            if len(distribution_dict_rokev) == 2:
             self.df['HerbName'] = self.df.apply(lambda row: row[self.columns['שם צמח']].strip() + ' ' 'לא מורכב' if ('ל' in row[self.columns[rokev]]) and ('ל' in row[self.columns[rokev]]) and ('א' in row[self.columns[rokev]]) else row[self.columns['שם צמח']].strip() + ' ' + 'מורכב', axis=1)
            else: 
              self.df['HerbName'] = self.df.apply(lambda row: row[self.columns[rokev]].strip() + ' ' + row[self.columns['שם צמח']].strip(), axis=1)
        
        elif (knh is not None) and (rokev is not None): 
          self.df['HerbName'] = self.df.apply(lambda row: row[self.columns[rokev]].strip() + ' ' + row[self.columns[knh]].strip(), axis=1)
        
        elif (knh is not None) or (rokev is not None):
            if rokev is not None:
                 self.df['HerbName'] = str(self.df[self.columns[rokev]]).strip()
            else:
                 self.df['HerbName'] = str(self.df[self.columns[knh]]).strip()
                
        else:
          raise ValueError('missing herb name information, please add the missing information to the file and try again.')
          
        
        none_count = self.df['HerbName'].isna().sum()
        if none_count > 0:
          authority = input('There is {none_count} out of {self.n} records with missing herb name information, please correct the values or give me authority to delete those rows: ')
          if authority == 'yes':
            self.df = self.df.loc[self.df['HerbName'].notnull()]
            self.n = len(self.df)
            if self.n == 0:
                  raise ValueError("No valid rows founded to integrate or you gave the autherity to drop all the rows.")
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
                    
class Validation_Quality:
    def __init__(self, df):
        self.df = df
        self.columns = columns_mapper(self.df) 
        self.n = len(self.df)
        
    
    def validate_wieght(self, thresh=3.2):
        col = self.columns['משקל weight']
        invalid_rows_df = self.df[self.df[col].apply(lambda x: math.log10(x)) > thresh]
        if not invalid_rows_df.empty:
            authority = input(f"{len(invalid_rows_df)} out of {self.n} Invalid weights value(s) founded in the weight column ({col}). the log 10 of the weight value must be less than {thresh}, give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i+2 for i in list(invalid_rows_df.index)]
                print(f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid weight value(s) founded in the weight column ({col}).")
        
        
    
    def validate_vine_freshness(self, low=0, up=5):
        col = self.columns['רעננות שדרהvine freshness']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        
        if not invalid_rows_df.empty:
            authority = input(f"{len(invalid_rows_df)} out of {self.n} Invalid vine freshness value(s) founded in the vine freshness column ({col}). the values must be in range({low}, {up}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i+2 for i in list(invalid_rows_df.index)]
                print(f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid vine freshness value(s) founded in the vine freshness column ({col}).")
            
        
    def validate_vine_root(self, low=0, up=5):
        col = self.columns['רקבון שזרהvine rot']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            authority = input(f"{len(invalid_rows_df)} out of {self.n} Invalid vine root value(s) founded in the vine root column ({col}). the values must be in range({low}, {up}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i+2 for i in list(invalid_rows_df.index)]
                print(f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid vine root value(s) founded in the vine root column ({col}).")
        
    def validate_shade(self, low=0, up=5):
        col = self.columns['גווןshade']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            authority = input(f"{len(invalid_rows_df)} out of {self.n} Invalid shade value(s) founded in the shade column ({col}). the values must be in range({low}, {up}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i+2 for i in list(invalid_rows_df.index)]
                print(f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid shade value(s) founded in the shade column ({col}).")
        
    def validate_general_appereance(self, low=0, up=5):
        col = self.columns['מראה כלליgeneral appearance']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            authority = input(f"{len(invalid_rows_df)} out of {self.n} Invalid general appereance value(s) founded in the general appereance column ({col}). the values must be in range({low}, {up}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i+2 for i in list(invalid_rows_df.index)]
                print(f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid general appereance value(s) founded in the general appereance column ({col}).")
        
    def validate_color_virus(self, low=0, up=5):
        col = self.columns['וירוס צבעcolor virus']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            authority = input(f"{len(invalid_rows_df)} out of {self.n} color virus value(s) founded in the color virus column ({col}). the values must be in range({low}, {up}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i+2 for i in list(invalid_rows_df.index)]
                print(f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid color virus value(s) founded in the color virus column ({col}).")
        
        
    def validate_scratches_virus(self, low=0, up=5):
        col = self.columns['וירוס שריטותscratches virus']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            authority = input(f"{len(invalid_rows_df)} out of {self.n} scratches virus value(s) founded in the scratches virus column ({col}). the values must be in range({low}, {up}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i+2 for i in list(invalid_rows_df.index)]
                print(f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid scratches virus value(s) founded in the scratches virus column ({col}).")
        
class Validation_Yield:
    def __init__(self, df, single_harvest_number_thresh=0.5, single_harvest_thresh=0.5):
        self.columns = columns_mapper(df) 
        self.df = update_single_harvest_number(df, self.columns['יצוא בודדים'], self.columns['יצוא בודדים מספר'])
        self.n = len(self.df)
        self.thresh1 = single_harvest_number_thresh
        self.thresh2 = single_harvest_thresh
        
    
    def validate_single_harvest(self):
        col = self.columns['יצוא בודדים']
        data = self.df[self.df[col] != 0]
        n = len(data)
        number_of_unique_values = data[col].nunique()
        if number_of_unique_values / n < self.thresh2:
            authority = input(f"the system guesses that there is invalid values in the single harvest column ({col}), check the values and fix them or give me authority to go forword if you think that there is no problem with the column values: ")
            if authority != 'yes':
                raise ValueError(f"Invalid single harvest value(s) founded in the single harvest column ({col}).")
            
    def validate_single_harvest_number(self):
        col = self.columns['יצוא בודדים מספר']
        data_filtered = self.df[self.df[col] != 0]
        data = data_filtered[data_filtered != -1]
        n = len(data)
        number_of_unique_values = data[col].nunique()
        if number_of_unique_values / n < self.thresh1:
            authority = input(f"the system guesses that there is invalid values in the single harvest number column ({col}), check the values and fix them or give me authority to go forword if you think that there is no problem with the column values: ")
            if authority != 'yes':
                raise ValueError(f"Invalid single harvest value(s) founded in the single harvest number column ({col}).")
        
        
  
     
                    
class Cemical_Extraction:
    def __init__(self, df, experiment=None):
        self.df = df
        self.exp = experiment.strip()
        self.columns = columns_mapper(self.df)
            
    def extract_Cemical(self):
        self.df = remove_outliers_zscore(self.df, self.columns['TA'])
        self.df = remove_outliers_zscore(self.df, self.columns['TSS'])
        self.df = remove_outliers_iqr(self.df, self.columns['גלוקוז'])
          
        if self.exp:
          self.df['ExperimentType'] = self.exp
          if self.df[self.columns[self.exp]].isna().sum() > 0:
            self.df[self.columns[self.exp]].fillna('Control', inplace=True)
        
          self.df['ExperimentParameter'] = self.df[self.columns[self.exp]]
          cemical_df = pd.DataFrame({'SampleID': self.df[self.columns['מזהה דגימה']].astype(int), 'ExperimentType': self.df['ExperimentType'].astype(str), 'ExperimentParameter': self.df['ExperimentParameter'].astype(str), 'TSS': self.df[self.columns['TSS']].astype(float), 'TA': self.df[self.columns['TA']].astype(float), 'Glucose': self.df[self.columns['גלוקוז']].astype(float)}, index=self.df.index)
        else:
          cemical_df = pd.DataFrame({'SampleID': self.df[self.columns['מזהה דגימה']].astype(int), 'TSS': self.df[self.columns['TSS']].astype(float), 'TA': self.df[self.columns['TA']].astype(float), 'Glucose': self.df[self.columns['גלוקוז']].astype(float)}, index=self.df.index)
          
        return cemical_df
    
    
class Quality_Extraction:
    def __init__(self, df, experiment=None):
        self.df = df
        self.exp = experiment.strip()
        self.columns = columns_mapper(self.df)
        
    def extract_Quality(self):
        result_columns = ['משקל weight', 'רעננות שדרהvine freshness', 'נפליםfallen', 'מפוצציםcracked', 'מוצקיםfirm', 'גמישיםflexible', 'רכיםsoft', 'רקוביםrotten', 'חריגי צבעcolor defect', 'רקבון שזרהvine rot', 'חסריםmissing', 'גווןshade', 'מראה כלליgeneral appearance', 'וירוס צבעcolor virus', 'וירוס שריטותscratches virus']
        ordinal_columns = []
        for c in result_columns:
          self.df = remove_outliers_iqr(self.df, self.columns[c])
        
        Quality_df = pd.DataFrame({'SampleID': self.df[self.columns['מזהה דגימה']].astype(int),
                                     'Weight': self.df[self.columns['משקל weight']].astype(float),
                                     'FruietNumber': self.df[self.columns['מספר פרות']].astype(int),
                                     'VineFreshness': self.df[self.columns['רעננות שדרהvine freshness']].astype(int),
                                     'Fallen': self.df[self.columns['נפליםfallen']].astype(int),
                                     'Cracked': self.df[self.columns['מפוצציםcracked']].astype(int),
                                     'Frim': self.df[self.columns['מוצקיםfirm']].astype(int),
                                     'Flexible': self.df[self.columns['גמישיםflexible']].astype(int),
                                     'Soft': self.df[self.columns['רכיםsoft']].astype(int),
                                     'Rotten': self.df[self.columns['רקוביםrotten']].astype(int),
                                     'ColorDefect': self.df[self.columns['חריגי צבעcolor defect']].astype(int),
                                     'Missing': self.df[self.columns['חסריםmissing']].astype(int),
                                     'VineRot': self.df[self.columns['רקבון שזרהvine rot']].astype(float),
                                     'Shade': self.df[self.columns['גווןshade']].astype(float),
                                     'GeneralAppearance': self.df[self.columns['מראה כלליgeneral appearance']].astype(float),
                                     'ColorVirus': self.df[self.columns['וירוס צבעcolor virus']].astype(float),
                                     'ScratchesVirus': self.df[self.columns['וירוס שריטותscratches virus']].astype(float)}, index=self.df.index)
        
        
        if self.exp:
          self.df['ExperimentType'] = self.exp
          if self.df[self.columns[self.exp]].isna().sum() > 0:
            self.df[self.columns[self.exp]].fillna('Control', inplace=True)
        
          self.df['ExperimentParameter'] = self.df[self.columns[self.exp]]
          experiment_cols = {'ExperimentType': self.df['ExperimentType'].astype(str), 'ExperimentParameter': self.df['ExperimentParameter'].astype(str)}
          first_column_index = Quality_df.columns.get_loc(Quality_df.columns[0])
          # Add the new columns after the first column
          for col_name, col_data in experiment_cols.items():
              Quality_df.insert(first_column_index + 1, col_name, col_data)
          
          Quality_df = cap_values(Quality_df, ['Fallen',  'Cracked', 'Frim', 'Flexible', 'Soft',  'Rotten', 'ColorDefect', 'Missing'], 'FruietNumber')
        
        return Quality_df
        
  
                          
class Meta_Extraction:
    def __init__(self, df):
        self.df = df
        self.columns = columns_mapper(self.df)
        
    def extract_Meta(self):
        SampleID = self.df[self.columns['מזהה דגימה']].astype(int)
        ParcelID = self.df[self.columns['חלקה']].astype(int)
        SortingDate = pd.to_datetime(self.df[self.columns['תאריך מיון']], errors='coerce')
        HarvestDate = pd.to_datetime(self.df[self.columns['תאריך קטיף']], errors='coerce').dt.date
        HerbName = self.df['HerbName'].astype(str)
        ParcelSize = self.df[self.columns['גודל חלקה במר']].astype(float)
        SortingType = self.df[self.columns['סוג מיון']].astype(str)
        
        meta = pd.DataFrame({'SampleID': SampleID, 'ParcelID': ParcelID, 'SortingDate': SortingDate, 'HarvestDate': HarvestDate, 'HerbName': HerbName, 'ParcelSize': ParcelSize, 'SortingType': SortingType}, index=self.df.index)
        meta['SortingType'] = meta['SortingType'].apply(lambda x: x.replace(" ", "").lower())
        self.df = meta
        return meta
                      
                    
    
class Insertion:
    def __init__(self, meta, yield_df=None, quality_df=None, cemical_df=None, exp=None):
        self.meta = meta
        self.y = yield_df
        self.q = quality_df  
        self.c = cemical_df  
        self.exp = exp
        
    def insert(self):
        if len(self.meta) != 0:
            try:
                self.Insert_Meta()
                if self.exp:
                    if self.c is not None:
                        self.Insert_Cemical_With_Experiment()
                    if self.q is not None:
                        self.Insert_Quality_With_Experiment()
                    if self.y is not None:
                        self.Insert_Yield_With_Experiment()
                else:
                    if self.c is not None:
                        self.Insert_Cemical_Without_Experiment()
                    if self.q is not None:
                        self.Insert_Quality_Without_Experiment()
                    if self.y is not None:
                        self.Insert_Yield_Without_Experiment()
            
                print(f'File succesfully Integrated to the DBS. {len(self.meta)} rows inserted')
            
            except mysql.connector.Error as error:
                print('the data is already in the data base.')
        else:
            print('you gave the authirity to remove corrputed data which is all the file data.')
        
    def Insert_Meta(self):
        # Establish MySQL connection (replace with your connection details)
        connection = connect()
        cursor = connection.cursor()
        # Define your table name
        table_name = 'ParcelMetaData'
        sql = f"INSERT INTO {table_name} (SampleID,ParcelID,SortingDate,HarvestDate,HerbName,SortingType,ParcelSize) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        # Insert data into MySQL table
        try:
            for index, row in self.meta.iterrows():
                cursor.execute(sql, (row['SampleID'], row['ParcelID'], row['SortingDate'], row['HarvestDate'], row['HerbName'], row['SortingType'], row['ParcelSize']))
            # Commit the transaction
            connection.commit()
            print(f"{len(self.meta)} rows inserted successfully into MySQL table: ", table_name)
          
        except mysql.connector.Error as error:
            #raise(error)
            print("Error inserting data:", error)
          
        finally:
            # Close the connection
            connection.close()
            
            
    def Insert_Cemical_Without_Experiment(self):
        connection = connect()
        cursor = connection.cursor()
        # Define your table name
        table_name = 'Cemical_Without_Experiment'
        sql = f"INSERT INTO {table_name} (SampleID,TSS,TA,Glucose) VALUES (%s, %s, %s, %s)"
        # Insert data into MySQL table
        try:
            self.c.columns = ['SampleID','TSS','TA','Glucose']
            for index, row in self.c.iterrows():
                cursor.execute(sql, (row['SampleID'], row['TSS'], row['TA'], row['Glucose']))
            # Commit the transaction
            connection.commit()
            print(f"{len(self.c)} rows inserted successfully into MySQL table: ", table_name)
        
        except mysql.connector.Error as error:
            print("Error inserting data:", error)
        
        finally:
            # Close the connection
            connection.close()
      
    def Insert_Cemical_With_Experiment(self):
        # Establish MySQL connection (replace with your connection details)
        connection = connect()
        cursor = connection.cursor()
        # Define your table name
        table_name = 'Cemical_With_Experiment'
        sql = f"INSERT INTO {table_name} (SampleID,ExperimentType,ExperimentParameter,TSS,TA,Glucose) VALUES (%s, %s, %s, %s, %s, %s)"
        # Insert data into MySQL table
        try:
            self.c.columns = ['SampleID','ExperimentType','ExperimentParameter','TSS','TA','Glucose']
            for index, row in self.c.iterrows():
                cursor.execute(sql, (row['SampleID'], row['ExperimentType'], row['ExperimentParameter'], row['TSS'], row['TA'], row['Glucose']))
            # Commit the transaction
            connection.commit()
            print(f"{len(self.c)} rows inserted successfully into MySQL table: ", table_name)
        
        except mysql.connector.Error as error:
            print("Error inserting data:", error)
        
        finally:
            # Close the connection
            connection.close()
      
     

    def Insert_Quality_With_Experiment(self):
        # Establish MySQL connection (replace with your connection details)
        connection = connect()
        cursor = connection.cursor()
        # Define your table name
        table_name = 'Quality_With_Experiment'
        sql = f"INSERT INTO {table_name} (SampleID,ExperimentType,ExperimentParameter,Weight,FruietNumber,VineFreshness,Fallen,Cracked,Firm,Flexible,Soft,Rotten,ColorDefect,Missing,VineRot,Shade,GeneralAppearance,ColorVirus,ScratchesVirus) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)"
        # Insert data into MySQL table
        try:
            self.q.columns = ['SampleID','ExperimentType','ExperimentParameter','Weight','FruietNumber','VineFreshness','Fallen','Cracked','Firm','Flexible','Soft','Rotten','ColorDefect','Missing','VineRot','Shade','GeneralAppearance','ColorVirus','ScratchesVirus']
            for index, r in self.q.iterrows():
                cursor.execute(sql, (r['SampleID'], r['ExperimentType'], r['ExperimentParameter'], r['Weight'],r['FruietNumber'],r['VineFreshness'],r['Fallen'],r['Cracked'],r['Firm'],r['Flexible'],r['Soft'],r['Rotten'],r['ColorDefect'],r['Missing'],r['VineRot'],r['Shade'],r['GeneralAppearance'],r['ColorVirus'],r['ScratchesVirus']))
            # Commit the transaction
            connection.commit()
            print(f"{len(self.q)} rows inserted successfully into MySQL table: ", table_name)
        
        except mysql.connector.Error as error:
            print("Error inserting data:", error)
        
        finally:
            # Close the connection
            connection.close()
      
    def Insert_Quality_Without_Experiment(self):
        connection = connect()
        cursor = connection.cursor()
        # Define your table name
        table_name = 'Quality_Without_Experiment'
        sql = f"INSERT INTO {table_name} (SampleID,Weight,FruietNumber,VineFreshness,Fallen,Cracked,Firm,Flexible,Soft,Rotten,ColorDefect,Missing,VineRot,Shade,GeneralAppearance,ColorVirus,ScratchesVirus) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)"
        # Insert data into MySQL table
        try:
            self.q.columns = ['SampleID','Weight','FruietNumber','VineFreshness','Fallen','Cracked','Firm','Flexible','Soft','Rotten','ColorDefect','Missing','VineRot','Shade','GeneralAppearance','ColorVirus','ScratchesVirus']
            for index, r in self.q.iterrows():
                cursor.execute(sql, (r['SampleID'],r['Weight'],r['FruietNumber'],r['VineFreshness'],r['Fallen'],r['Cracked'],r['Firm'],r['Flexible'],r['Soft'],r['Rotten'],r['ColorDefect'],r['Missing'],r['VineRot'],r['Shade'],r['GeneralAppearance'],r['ColorVirus'],r['ScratchesVirus']))
            # Commit the transaction
            connection.commit()
            print(f"{len(self.q)} rows inserted successfully into MySQL table: ", table_name)
        
        except mysql.connector.Error as error:
            print("Error inserting data:", error)
        
        finally:
            # Close the connection
            connection.close()
      
      


    def Insert_Yield_With_Experiment(self):
        # Establish MySQL connection (replace with your connection details)
        connection = connect()
        cursor = connection.cursor()
        # Define your table name
        table_name = 'Yield_With_Experiment'
        sql = f"INSERT INTO {table_name} (SampleID,ExperimentType,ExperimentParameter,ClusterHarvesd,SingleHarvesd,SingleHarvesdNumber,GreenFruits,CrackedFruits,BlackPitDefect,BlackPitDefectNumber,Others,PlantProtection,Virus) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)"
        # Insert data into MySQL table
        try:
            self.y.replace({np.nan: None}, inplace=True)
            self.y.columns = ['SampleID','ExperimentType','ExperimentParameter','ClusterHarvesd','SingleHarvesd','SingleHarvesdNumber','GreenFruits','CrackedFruits','BlackPitDefect','BlackPitDefectNumber','Others','PlantProtection','Virus']
            for index, row in self.y.iterrows():
                cursor.execute(sql, (row['SampleID'], row['ExperimentType'], row['ExperimentParameter'], row['ClusterHarvesd'], row['SingleHarvesd'], row['SingleHarvesdNumber'], row['GreenFruits'], row['CrackedFruits'], row['BlackPitDefect'], row['BlackPitDefectNumber'], row['Others'], row['PlantProtection'], row['Virus']))
            # Commit the transaction
            connection.commit()
            print(f"{len(self.y)} rows inserted successfully into MySQL table: ", table_name)
        
        except mysql.connector.Error as error:
            print("Error inserting data:", error)
        
        finally:
            # Close the connection
            connection.close()
      
    def Insert_Yield_Without_Experiment(self):
        # Establish MySQL connection (replace with your connection details)
        connection = connect()
        cursor = connection.cursor()
        # Define your table name
        table_name = 'Yield_Without_Experiment'
        sql = f"INSERT INTO {table_name} (SampleID,ClusterHarvesd,SingleHarvesd,SingleHarvesdNumber,GreenFruits,CrackedFruits,BlackPitDefect,BlackPitDefectNumber,Others,PlantProtection,Virus) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)"
        # Insert data into MySQL table
        try:
            self.y.replace({np.nan: None}, inplace=True)
            self.y.columns = ['SampleID','ClusterHarvesd','SingleHarvesd','SingleHarvesdNumber','GreenFruits','CrackedFruits','BlackPitDefect','BlackPitDefectNumber','Others','PlantProtection','Virus']
            for index, row in self.y.iterrows():
                cursor.execute(sql, (row['SampleID'], row['ClusterHarvesd'], row['SingleHarvesd'], row['SingleHarvesdNumber'], row['GreenFruits'], row['CrackedFruits'], row['BlackPitDefect'], row['BlackPitDefectNumber'], row['Others'], row['PlantProtection'], row['Virus']))
            # Commit the transaction
            connection.commit()
            print(f"{len(self.y)} rows inserted successfully into MySQL table: ", table_name)
        
        except mysql.connector.Error as error:
            print("Error inserting data:", error)
        
        finally:
            # Close the connection
            connection.close()

class Yield_Extraction:
    def __init__(self, df, experiment=None):
        self.df = df
        self.exp = experiment.strip()
        self.columns = columns_mapper(self.df)
        
    def extract_Yield(self):
        result_columns = ['יצוא אשכולות', 'יצוא בודדים', 'יצוא בודדים מספר', 'ירוקים', 'סדוקים', 'שחור פיטם', 'שחור פיטם מספר', 'אחרים', 'הגנת הצומח', 'וירוס']
        self.df = FruietNumber_Integrator(self.df, self.columns['יצוא בודדים מספר'])
        
        for c in result_columns:
          self.df = remove_outliers_iqr(self.df, self.columns[c])
        
        Yield_df = pd.DataFrame({'SampleID': self.df[self.columns['מזהה דגימה']].astype(int),
                                     'ClusterHarvesd': self.df[self.columns['יצוא אשכולות']].astype(float),
                                     'SingleHarvesd': self.df[self.columns['יצוא בודדים']].astype(float),
                                     'SingleHarvesdNumber': self.df[self.columns['יצוא בודדים מספר']],
                                     'GreenFruits': self.df[self.columns['ירוקים']].astype(float),
                                     'CrackedFruits': self.df[self.columns['סדוקים']].astype(float),
                                     'BlackPitDefect': self.df[self.columns['שחור פיטם']].astype(float),
                                     'BlackPitDefectNumber': self.df[self.columns['שחור פיטם מספר']].astype(int),
                                     'Others': self.df[self.columns['אחרים']].astype(float),
                                     'PlantProtection': self.df[self.columns['הגנת הצומח']].astype(float),
                                     'Virus': self.df[self.columns['וירוס']].astype(float)}, index=self.df.index)
        if self.exp:
          self.df['ExperimentType'] = self.exp
          if self.df[self.columns[self.exp]].isna().sum() > 0:
            self.df[self.columns[self.exp]].fillna('Control', inplace=True)
        
          self.df['ExperimentParameter'] = self.df[self.columns[self.exp]]
          experiment_cols = {'ExperimentType': self.df['ExperimentType'].astype(str), 'ExperimentParameter': self.df['ExperimentParameter'].astype(str)}
          first_column_index = Yield_df.columns.get_loc(Yield_df.columns[0])
          # Add the new columns after the first column
          for col_name, col_data in experiment_cols.items():
              Yield_df.insert(first_column_index + 1, col_name, col_data)
              
        return Yield_df

if __name__ == "__main__":
    # Example Usage:
    # tometo1/Experiment163Results.csv
    data = pd.read_csv('C:/Users/ASUS/Desktop/tometo1/Experiment108Results.csv')
    experiment_user = input("Please enter Experiment column name if there is an Experiment in the file: ")
    exp = ' '.join(experiment_user.split())
    columns = columns_mapper(data)
    data[columns['סוג מיון']] = data[columns['סוג מיון']].apply(lambda x: x.replace(" ", "").lower())
    
    validator_meta = Validation_Meta(data)
    validator_meta.validate_sampleID()
    validator_meta.validate_harvest_date()
    validator_meta.validate_sorting_date()
    validator_meta.validate_date_gap()
    validator_meta.validate_parcel_size()
    validator_meta.validate_exists_herb_name_info()
    validator_meta.validate_herb_name_info_with_dbms()
    
    Meta_Extractor = Meta_Extraction(validator_meta.df)
    meta = Meta_Extractor.extract_Meta()
    
    c, q, y = None, None, None
    sorting_types = list(validator_meta.df[columns['סוג מיון']].unique())
    sorting_types = [s.replace(" ", "").lower() for s in sorting_types]
    for s in sorting_types:
        if s in ['cemical', 'כימי', 'cemicalכימי', 'כימיcemical']:
            c_df = validator_meta.df[validator_meta.df[columns['סוג מיון']] == s]
            Cemical_Extractor = Cemical_Extraction(c_df, exp)
            c = Cemical_Extractor.extract_Cemical()
            
        elif s in ['quality', 'איכות', 'qualityאיכות', 'איכותquality']:
            q_df = validator_meta.df[validator_meta.df[columns['סוג מיון']] == s]
            Validateor_Quality = Validation_Quality(q_df)
            Validateor_Quality.validate_wieght()
            Validateor_Quality.validate_vine_root()
            Validateor_Quality.validate_vine_freshness()
            Validateor_Quality.validate_shade()
            Validateor_Quality.validate_scratches_virus()
            Validateor_Quality.validate_general_appereance()
            Validateor_Quality.validate_color_virus()
            Quality_Extractor = Quality_Extraction(Validateor_Quality.df, exp)
            q = Quality_Extractor.extract_Quality()
            
        elif s in ['yield', 'רגיל', 'yieldרגיל', 'רגילyield']:
            y_df = validator_meta.df[validator_meta.df[columns['סוג מיון']] == s]
            Validateor_Yield = Validation_Yield(y_df)
            Validateor_Yield.validate_single_harvest()
            Validateor_Yield.validate_single_harvest_number()
            Yield_Extractor = Yield_Extraction(Validateor_Yield.df, exp)
            y = Yield_Extractor.extract_Yield()
            
        else:
            raise ValueError(f'error sorting type {s} not integrated with the sysytem')
    

    meta_data = Meta_to_keep(c, q, y, meta)       
    Inserter = Insertion(meta_data, yield_df=y, quality_df=q, cemical_df=c, exp=exp)    
    Inserter.insert()       
    
    



